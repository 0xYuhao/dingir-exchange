#![allow(clippy::if_same_then_else)]
use crate::asset::{BalanceManager, BalanceType, BalanceUpdateController, BalanceUpdateParams, BusinessType};
use crate::config::{self, OrderSignatrueCheck};
use crate::persist::PersistExector;
use crate::sequencer::Sequencer;
use crate::types::{self, MarketRole, OrderEventType};

use std::cmp::min;
use std::collections::BTreeMap;
use std::iter::Iterator;

use anyhow::{bail, Result};
use fluidex_common::rust_decimal::prelude::Zero;
use fluidex_common::rust_decimal::{Decimal, RoundingStrategy};
use fluidex_common::utils::timeutil::current_timestamp;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

pub use types::{OrderSide, OrderType};

mod order;
pub use order::*;
mod trade;
pub use trade::*;

// Market - 表示一个交易市场
pub struct Market {
    pub name: &'static str,  // 市场名称
    pub base: &'static str,  // 基础货币
    pub quote: &'static str, // 报价货币
    pub amount_prec: u32,    // 数量精度
    pub price_prec: u32,     // 价格精度
    pub base_prec: u32,      // 基础货币精度
    pub quote_prec: u32,     // 报价货币精度
    pub fee_prec: u32,       // 手续费精度
    pub min_amount: Decimal, // 最小交易数量
    pub price: Decimal,      // 当前价格

    pub orders: BTreeMap<u64, OrderRc>,               // 所有订单
    pub users: BTreeMap<u32, BTreeMap<u64, OrderRc>>, // 用户订单映射
    //pub struct MarketKeyAsk {
    //     pub order_price: Decimal,
    //     pub order_id: u64,
    // }
    pub asks: BTreeMap<MarketKeyAsk, OrderRc>, // 卖单队列 (价格从低到高排序)
    //pub struct MarketKeyBid {
    //     pub order_price: Decimal,
    //     pub order_id: u64,
    // }
    pub bids: BTreeMap<MarketKeyBid, OrderRc>, // 买单队列 (价格从高到低排序)

    pub trade_count: u64, // 成交数量

    pub disable_self_trade: bool,                  // 是否禁止自成交
    pub disable_market_order: bool,                // 是否禁止市价单
    pub check_eddsa_signatue: OrderSignatrueCheck, // 签名验证设置
}

pub struct BalanceManagerWrapper<'a> {
    pub inner: &'a mut BalanceManager,
}

impl<'a> From<&'a mut BalanceManager> for BalanceManagerWrapper<'a> {
    fn from(origin: &'a mut BalanceManager) -> Self {
        BalanceManagerWrapper { inner: origin }
    }
}

impl BalanceManagerWrapper<'_> {
    pub fn balance_add(&mut self, user_id: u32, balance_type: BalanceType, asset: &str, amount: &Decimal) {
        self.inner.add(user_id, balance_type, asset, amount);
    }
    pub fn balance_get(&mut self, user_id: u32, balance_type: BalanceType, asset: &str) -> Decimal {
        self.inner.get(user_id, balance_type, asset)
    }
    pub fn balance_total(&mut self, user_id: u32, asset: &str) -> Decimal {
        self.inner.get(user_id, BalanceType::FREEZE, asset) + self.inner.get(user_id, BalanceType::AVAILABLE, asset)
    }
    pub fn balance_sub(&mut self, user_id: u32, balance_type: BalanceType, asset: &str, amount: &Decimal) {
        self.inner.sub(user_id, balance_type, asset, amount);
    }
    pub fn balance_frozen(&mut self, user_id: u32, asset: &str, amount: &Decimal) {
        self.inner.frozen(user_id, asset, amount)
    }
    pub fn balance_unfrozen(&mut self, user_id: u32, asset: &str, amount: &Decimal) {
        self.inner.unfrozen(user_id, asset, amount)
    }
    pub fn asset_prec(&mut self, asset: &str) -> u32 {
        self.inner.asset_manager.asset_prec(asset)
    }
}

const MAP_INIT_CAPACITY: usize = 1024;

// TODO: is it ok to match with oneself's order?
// TODO: precision
impl Market {
    // 创建新市场
    pub fn new(market_conf: &config::Market, global_settings: &config::Settings, balance_manager: &BalanceManager) -> Result<Market> {
        // 定义两个闭包函数用于检查资产
        // 检查资产是否存在
        let asset_exist = |asset: &str| -> bool { balance_manager.asset_manager.asset_exist(asset) };
        // 获取资产精度
        let asset_prec = |asset: &str| -> u32 { balance_manager.asset_manager.asset_prec(asset) };

        // 检查交易对中的基础货币和报价货币是否存在
        if !asset_exist(&market_conf.quote) || !asset_exist(&market_conf.base) {
            bail!("invalid assert id {} {}", market_conf.quote, market_conf.base);
        }

        // 获取基础货币和报价货币的精度
        let base_prec = asset_prec(&market_conf.base);
        let quote_prec = asset_prec(&market_conf.quote);

        // 验证交易精度设置是否合理:
        // 1. 数量精度不能大于基础货币精度
        // 2. 数量精度+价格精度不能大于报价货币精度
        if market_conf.amount_prec > base_prec || market_conf.amount_prec + market_conf.price_prec > quote_prec {
            bail!("invalid precision");
        }

        // 是否允许手续费精度取整
        let allow_rounding_fee = true;
        // 如果不允许手续费精度取整,则需要验证:
        // 1. 数量精度+手续费精度不能大于基础货币精度
        // 2. 数量精度+价格精度+手续费精度不能大于报价货币精度
        if !allow_rounding_fee {
            if market_conf.amount_prec + market_conf.fee_prec > base_prec
                || market_conf.amount_prec + market_conf.price_prec + market_conf.fee_prec > quote_prec
            {
                bail!("invalid fee precision");
            }
        }

        // 将字符串转换为'static生命周期的字符串引用
        let leak_fn = |x: &str| -> &'static str { Box::leak(x.to_string().into_boxed_str()) };
        let market = Market {
            name: leak_fn(&market_conf.name),
            base: leak_fn(&market_conf.base),
            quote: leak_fn(&market_conf.quote),
            amount_prec: market_conf.amount_prec,
            price_prec: market_conf.price_prec,
            base_prec,
            quote_prec,
            fee_prec: market_conf.fee_prec,
            min_amount: market_conf.min_amount,
            price: Decimal::zero(),
            orders: BTreeMap::new(),
            users: BTreeMap::new(),
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            trade_count: 0,
            disable_self_trade: global_settings.disable_self_trade,
            disable_market_order: global_settings.disable_market_order,
            check_eddsa_signatue: global_settings.check_eddsa_signatue,
        };
        Ok(market)
    }

    // 重置市场状态
    pub fn reset(&mut self) {
        log::debug!("market {} reset", self.name);
        self.bids.clear();
        self.asks.clear();
        self.users.clear();
        self.orders.clear();
    }
    // 冻结用户余额
    pub fn frozen_balance(&self, balance_manager: &mut BalanceManagerWrapper<'_>, order: &Order) {
        let asset = if order.is_ask() { &self.base } else { &self.quote };

        balance_manager.balance_frozen(order.user, asset, &order.frozen);
    }
    // 解冻用户余额
    pub fn unfrozen_balance(&self, balance_manager: &mut BalanceManagerWrapper<'_>, order: &Order) {
        debug_assert!(order.remain.is_sign_positive());
        if order.remain.is_zero() {
            return;
        }
        let asset = if order.is_ask() { &self.base } else { &self.quote };
        balance_manager.balance_unfrozen(order.user, asset, &order.frozen);
    }

    // 处理订单的主要函数
    pub fn put_order(
        &mut self,
        sequencer: &mut Sequencer,                               // 序列生成器,用于生成订单ID
        mut balance_manager: BalanceManagerWrapper<'_>,          // 余额管理器
        balance_update_controller: &mut BalanceUpdateController, // 余额更新控制器
        persistor: &mut impl PersistExector,                     // 持久化执行器
        order_input: OrderInput,                                 // 输入的订单信息
    ) -> Result<Order> {
        // 1. 订单基本验证
        // 检查是否允许市价单
        if order_input.type_ == OrderType::MARKET && self.disable_market_order {
            bail!("market orders disabled");
        }
        // 检查订单数量是否达到最小要求
        if order_input.amount.lt(&self.min_amount) {
            bail!("invalid amount");
        }
        // fee_prec == 0 means no fee allowed
        // 如果手续费精度为0，则不允许设置手续费
        if self.fee_prec == 0 && (!order_input.taker_fee.is_zero() || !order_input.maker_fee.is_zero()) {
            bail!("only 0 fee is supported now");
        }

        // 2. 精度处理
        // 处理数量精度 （防御性编程，这是要求输入的值已经是正确的精度了）
        let amount = order_input
            .amount
            .round_dp_with_strategy(self.amount_prec, RoundingStrategy::ToZero);
        //（防御性编程，这是要求输入的值已经是正确的精度了）
        if amount != order_input.amount {
            bail!("invalid amount precision");
        }
        // 处理价格精度
        let price = order_input.price.round_dp(self.price_prec);
        if price != order_input.price {
            bail!("invalid price precision");
        }

        // 3. 市价单特殊验证
        if order_input.type_ == OrderType::MARKET {
            // 市价单不能设置价格
            if !order_input.price.is_zero() {
                bail!("market order should not have a price");
            }
            // 市价单不能设置post_only（市价单不能设置为仅作为挂单）
            if order_input.post_only {
                bail!("market order cannot be post only");
            }
            // 市价单必须有对手单
            if order_input.side == OrderSide::ASK && self.bids.is_empty() || order_input.side == OrderSide::BID && self.asks.is_empty() {
                bail!("no counter orders");
            }
        } else if order_input.price.is_zero() {
            // 限价单必须设置价格
            bail!("invalid price for limit order");
        }

        // 4. 余额检查
        if order_input.side == OrderSide::ASK {
            // 卖单检查base资产余额
            if balance_manager
                .balance_get(order_input.user_id, BalanceType::AVAILABLE, self.base)
                .lt(&order_input.amount)
            {
                bail!("balance not enough");
            }
        } else {
            // 买单检查quote资产余额
            let balance = balance_manager.balance_get(order_input.user_id, BalanceType::AVAILABLE, self.quote);

            if order_input.type_ == OrderType::LIMIT {
                // 限价买单需要检查 数量*价格 是否超过余额
                if balance.lt(&(order_input.amount * order_input.price)) {
                    bail!(
                        "balance not enough: balance({}) < amount({}) * price({})",
                        &balance,
                        &order_input.amount,
                        &order_input.price
                    );
                }
            } else {
                // We have already checked that counter order book is not empty,
                // so `unwrap` here is safe.
                // Here we only make a minimum balance check against the top of the counter order book.
                // After the check, balance may still be not enough, then the remain part of the order
                // will be marked as `canceled(finished)`.

                // update 2021.06.22: we now allow market order to partially fill a counter order
                // so we don't need the check now
                //let top_counter_order_price = self.asks.values().next().unwrap().borrow().price;
                //if balance.lt(&(order_input.amount * top_counter_order_price)) {
                //    bail!("balance not enough");
                //}

                // 我们已经检查了对手订单簿不为空,
                // 所以这里的 `unwrap` 是安全的。
                // 这里我们只对订单簿顶部的对手单做最小余额检查。
                // 在检查之后,余额可能仍然不足,那么订单的剩余部分
                // 将被标记为 `canceled(finished)`。

                // 更新于 2021.06.22: 我们现在允许市价单部分成交一个对手单
                // 所以我们现在不需要这个检查了
                //let top_counter_order_price = self.asks.values().next().unwrap().borrow().price;
                //if balance.lt(&(order_input.amount * top_counter_order_price)) {
                //    bail!("balance not enough");
                //}
            }
            // 市价买单的余额检查已被移除,允许部分成交
        }

        // 5. 设置quote限制(仅用于市价买单)
        let quote_limit = if order_input.type_ == OrderType::MARKET && order_input.side == OrderSide::BID {
            let balance = balance_manager.balance_get(order_input.user_id, BalanceType::AVAILABLE, self.quote);
            if order_input.quote_limit.is_zero() {
                // quote_limit == 0 means no extra limit
                // 如果quote_limit为0，则直接使用余额作为quote限制
                // quote_limit为0表示无额外限制
                balance
            } else {
                // 取余额和quote_limit的较小值
                std::cmp::min(
                    balance,
                    order_input
                        .quote_limit
                        .round_dp_with_strategy(balance_manager.asset_prec(self.quote), RoundingStrategy::ToZero),
                )
            }
        } else {
            // not used
            Decimal::zero()
        };

        // 6. 创建订单对象
        let t = current_timestamp();
        let order = Order {
            id: sequencer.next_order_id(),
            type_: order_input.type_,         // 订单类型(市价单/限价单)
            side: order_input.side,           // 订单方向(买/卖)
            create_time: t,                   // 创建时间
            update_time: t,                   // 更新时间
            market: self.name.into(),         // 市场名称
            base: self.base.into(),           // 基础货币
            quote: self.quote.into(),         // 报价货币
            user: order_input.user_id,        // 用户ID
            price: order_input.price,         // 价格
            amount: order_input.amount,       // 数量
            taker_fee: order_input.taker_fee, // taker手续费率
            maker_fee: order_input.maker_fee, // maker手续费率
            remain: order_input.amount,       // 剩余未成交数量
            frozen: Decimal::zero(),          // 冻结金额
            finished_base: Decimal::zero(),   // 已成交基础货币数量
            finished_quote: Decimal::zero(),  // 已成交报价货币数量
            finished_fee: Decimal::zero(),    // 已成交手续费
            post_only: order_input.post_only, // 是否仅做挂单(post_only)
            signature: order_input.signature, // 签名
        };

        // 7. 执行订单撮合
        let order = self.execute_order(
            sequencer,
            &mut balance_manager,
            balance_update_controller,
            persistor,
            order,
            &quote_limit,
        );
        Ok(order)
    }

    // the last parameter `quote_limit`, is only used for market bid order,
    // it indicates the `quote` balance of the user,
    // so the sum of all the trades' quote amount cannot exceed this value
    // 执行订单撮合
    fn execute_order(
        &mut self,
        sequencer: &mut Sequencer,
        balance_manager: &mut BalanceManagerWrapper<'_>,
        balance_update_controller: &mut BalanceUpdateController,
        persistor: &mut impl PersistExector,
        mut taker: Order,
        quote_limit: &Decimal,
    ) -> Order {
        log::debug!("execute_order {:?}", taker);

        // the the older version, PUT means being inserted into orderbook
        // so if an order is matched instantly, only 'FINISH' event will occur, no 'PUT' event
        // now PUT means being created
        // we can revisit this decision later
        // 记录订单创建事件
        persistor.put_order(&taker, OrderEventType::PUT);

        // 设置订单类型标志
        // taker是用户的订单,maker是对手方的订单
        let taker_is_ask = taker.side == OrderSide::ASK; // taker是否为卖单
        let taker_is_bid = !taker_is_ask; // taker是否为买单
        let maker_is_bid = taker_is_ask; // maker是否为买单
        let maker_is_ask = !maker_is_bid; // maker是否为卖单
        let is_limit_order = taker.type_ == OrderType::LIMIT; // 是否为限价单
        let is_market_order = !is_limit_order; // 是否为市价单
        let is_post_only_order = taker.post_only; // 是否为只挂单订单

        let mut quote_sum = Decimal::zero(); // 累计成交的报价金额
        let mut finished_orders = Vec::new(); // 已完成订单列表

        // 获取对手方订单列表迭代器
        let counter_orders: Box<dyn Iterator<Item = &mut OrderRc>> = if maker_is_bid {
            Box::new(self.bids.values_mut()) // 如果maker是买单,获取买单列表
        } else {
            Box::new(self.asks.values_mut()) // 如果maker是卖单,获取卖单列表
        };

        // TODO: find a more elegant way to handle this
        // 是否需要取消订单的标志
        let mut need_cancel = false;

        // 遍历对手方订单进行撮合
        for maker_ref in counter_orders {
            // Step1: get ask and bid
            // 步骤1: 获取买卖双方订单
            let mut maker = maker_ref.borrow_mut(); // borrow_mut 获取对手方订单的写锁
            if taker.remain.is_zero() {
                break; // taker已完全成交,退出循环
            }

            // 获取买卖双方手续费率
            let (ask_fee_rate, bid_fee_rate) = if taker_is_ask {
                (taker.taker_fee, maker.maker_fee)
            } else {
                (maker.maker_fee, taker.taker_fee)
            };
            // of course, price should be counter order price
            // 以maker的价格为成交价
            let price = maker.price;
            // 确定买卖双方订单
            let (ask_order, bid_order) = if taker_is_ask {
                (&mut taker, &mut *maker)
            } else {
                (&mut *maker, &mut taker)
            };
            //let ask_order_id: u64 = ask_order.id;
            //let bid_order_id: u64 = bid_order.id;

            // Step2: abort if needed
            // 如果taker是限价单且maker的卖价高于taker的买价,则无法成交
            // 因为卖单队列的价格是从低到高排序，如果当前maker的卖价高于taker的买价,则无法成交,可以直接中断循环
            if is_limit_order && ask_order.price.gt(&bid_order.price) {
                break; // 限价单且卖价高于买价,无法成交
            }
            // new trade will be generated
            // 如果taker是只挂单订单且遇到可成交订单,需要取消taker订单
            if is_post_only_order {
                need_cancel = true; // 只挂单订单遇到可成交订单需要取消
                break;
            }
            // 如果taker和maker是同一个用户,且禁止自成交,需要取消taker订单
            if ask_order.user == bid_order.user && self.disable_self_trade {
                need_cancel = true; // 自成交且禁止自成交,需要取消
                break;
            }

            // Step3: get trade amount
            // 计算成交数量
            let mut traded_base_amount = min(ask_order.remain, bid_order.remain);
            // 市价买单需要检查报价限制
            if taker_is_bid && is_market_order {
                // 检查当前成交金额是否会超出报价限制 （quote_sum 当前已占用的报价金额，市价单专属，初始为0）
                if (quote_sum + price * traded_base_amount).gt(quote_limit) {
                    // divide remain quote by price to get a base amount to be traded,
                    // so quote_limit will be `almost` fulfilled
                    // 将剩余报价除以价格,得到可成交的基础货币数量,
                    // 这样报价限制将接近满足
                    // 如果超出报价限制,按剩余报价限制计算可成交数量
                    let remain_quote_limit = quote_limit - quote_sum;
                    traded_base_amount = (remain_quote_limit / price).round_dp_with_strategy(self.amount_prec, RoundingStrategy::ToZero);
                    if traded_base_amount.is_zero() {
                        break;
                    }
                }
            }
            let traded_quote_amount = price * traded_base_amount;
            debug_assert!(!traded_base_amount.is_zero());
            debug_assert!(!traded_quote_amount.is_zero());
            quote_sum += traded_quote_amount;
            if taker_is_bid && is_market_order {
                debug_assert!(quote_sum <= *quote_limit);
            }

            // Step4: create the trade
            // 步骤4: 创建成交记录
            // 计算买方手续费是 成交数量 * 买方手续费率  （成交数量是基础货币数量）
            let bid_fee = (traded_base_amount * bid_fee_rate).round_dp_with_strategy(self.base_prec, RoundingStrategy::ToZero);
            // 计算卖方手续费是 成交金额 * 卖方手续费率  （成交金额是报价货币数量）
            let ask_fee = (traded_quote_amount * ask_fee_rate).round_dp_with_strategy(self.quote_prec, RoundingStrategy::ToZero);

            // 更新订单时间戳
            let timestamp = current_timestamp();
            ask_order.update_time = timestamp;
            bid_order.update_time = timestamp;

            // emit the trade
            let trade_id = sequencer.next_trade_id();
            // 创建成交记录
            let trade = Trade {
                id: trade_id,                      // 交易ID,由序列生成器生成
                timestamp: current_timestamp(),    // 交易发生的时间戳
                market: self.name.to_string(),     // 交易市场名称
                base: self.base.into(),            // 基础货币
                quote: self.quote.into(),          // 报价货币
                price,                             // 成交价格
                amount: traded_base_amount,        // 成交数量(基础货币)
                quote_amount: traded_quote_amount, // 成交金额(报价货币)

                // 卖方信息
                ask_user_id: ask_order.user, // 卖方用户ID
                ask_order_id: ask_order.id,  // 卖方订单ID
                ask_role: if taker_is_ask {
                    // 用户单角色(Taker/Maker)
                    MarketRole::TAKER
                } else {
                    MarketRole::MAKER
                },
                ask_fee, // 卖方手续费

                // 买方信息
                bid_user_id: bid_order.user, // 买方用户ID
                bid_order_id: bid_order.id,  // 买方订单ID
                bid_role: if taker_is_ask {
                    // 对手单角色(Taker/Maker)
                    MarketRole::MAKER
                } else {
                    MarketRole::TAKER
                },
                bid_fee, // 买方手续费

                // 可选字段
                ask_order: None, // 卖方订单完整信息(可选)
                bid_order: None, // 买方订单完整信息(可选)

                // 仅在启用 emit_state_diff 特性时包含
                #[cfg(feature = "emit_state_diff")]
                state_before: Default::default(), // 交易前状态
                #[cfg(feature = "emit_state_diff")]
                state_after: Default::default(), // 交易后状态
            };
            #[cfg(feature = "emit_state_diff")]
            let state_before = Self::get_trade_state(ask_order, bid_order, balance_manager, self.base, self.quote);
            self.trade_count += 1;
            if self.disable_self_trade {
                debug_assert_ne!(trade.ask_user_id, trade.bid_user_id);
            }

            // Step5: update orders
            // 更新订单状态
            // 检查ask_order是否是新订单
            let ask_order_is_new = ask_order.finished_base.is_zero();
            // 检查bid_order是否是新订单
            let bid_order_is_new = bid_order.finished_base.is_zero();
            // 保存ask_order的原始状态
            let ask_order_before = *ask_order;
            // 保存bid_order的原始状态
            let bid_order_before = *bid_order;
            // 更新ask_order的剩余数量
            ask_order.remain -= traded_base_amount;
            debug_assert!(ask_order.remain.is_sign_positive());
            bid_order.remain -= traded_base_amount;
            debug_assert!(bid_order.remain.is_sign_positive());
            ask_order.finished_base += traded_base_amount;
            bid_order.finished_base += traded_base_amount;
            ask_order.finished_quote += traded_quote_amount;
            bid_order.finished_quote += traded_quote_amount;
            ask_order.finished_fee += ask_fee;
            bid_order.finished_fee += bid_fee;

            // Step6: update balances
            // 对于taker单，（用户主动发起的单子），不管买单还是卖单都用的是可用金额，但是作为对手单（maker单），如果是卖单，更新的是冻结金额，如果是买单更新的是可用金额。（也就是挂单的买单是不会冻结金额的）
            // 也就是买单类型，更新的是可用余额，卖单类型，如果是对手单（maker）更新的是冻结余额，如果是用户单（taker）更新的是可用余额
            // 更新买方基础资产余额 -- 更新的是可用余额 （加法）
            balance_update_controller
                .update_user_balance(
                    balance_manager.inner,
                    persistor,
                    BalanceUpdateParams {
                        balance_type: BalanceType::AVAILABLE,
                        business_type: BusinessType::Trade,
                        user_id: bid_order.user,
                        asset: self.base.to_string(),
                        business: "trade".to_string(),
                        business_id: trade_id,
                        market_price: self.price,
                        change: if bid_fee.is_sign_positive() {
                            traded_base_amount - bid_fee // 如果买单手续费为正,则减去手续费
                        } else {
                            traded_base_amount // 如果手续费为负,则不减去手续费
                        },
                        detail: serde_json::Value::default(), // 设置为 null 的详细信息字段,可用于记录额外的余额变动信息
                        signature: vec![],                    // 设置为空的签名字段
                    },
                )
                .unwrap();
            // 更新卖方基础资产余额 -- 如果卖方是对手单，更新的是冻结余额 （减法）
            balance_update_controller
                .update_user_balance(
                    balance_manager.inner,
                    persistor,
                    BalanceUpdateParams {
                        balance_type: if maker_is_ask {
                            BalanceType::FREEZE
                        } else {
                            BalanceType::AVAILABLE
                        },
                        business_type: BusinessType::Trade,
                        user_id: ask_order.user,
                        asset: self.base.to_string(),
                        business: "trade".to_string(),
                        business_id: trade_id,
                        market_price: self.price,
                        change: -traded_base_amount,
                        detail: serde_json::Value::default(),
                        signature: vec![],
                    },
                )
                .unwrap();
            // 更新卖方报价资产余额 -- 更新的是可用余额 （加法）
            balance_update_controller
                .update_user_balance(
                    balance_manager.inner,
                    persistor,
                    BalanceUpdateParams {
                        balance_type: BalanceType::AVAILABLE,
                        business_type: BusinessType::Trade,
                        user_id: ask_order.user,
                        asset: self.quote.to_string(),
                        business: "trade".to_string(),
                        business_id: trade_id,
                        market_price: self.price,
                        change: if ask_fee.is_sign_positive() {
                            traded_quote_amount - ask_fee
                        } else {
                            traded_quote_amount
                        },
                        detail: serde_json::Value::default(),
                        signature: vec![],
                    },
                )
                .unwrap();
            // 更新买方报价资产余额 -- 如果买方是对手单，更新的是冻结余额 （减法）
            balance_update_controller
                .update_user_balance(
                    balance_manager.inner,
                    persistor,
                    BalanceUpdateParams {
                        balance_type: if maker_is_bid {
                            BalanceType::FREEZE
                        } else {
                            BalanceType::AVAILABLE
                        },
                        business_type: BusinessType::Trade,
                        user_id: bid_order.user,
                        asset: self.quote.to_string(),
                        business: "trade".to_string(),
                        business_id: trade_id,
                        market_price: self.price,
                        change: -traded_quote_amount,
                        detail: serde_json::Value::default(),
                        signature: vec![],
                    },
                )
                .unwrap();
            #[cfg(feature = "emit_state_diff")]
            let state_after = Self::get_trade_state(ask_order, bid_order, balance_manager, self.base, self.quote);

            // Step7: persist trade and order
            //if true persistor.real_persist() {
            //if true
            let trade = Trade {
                #[cfg(feature = "emit_state_diff")]
                state_after,
                #[cfg(feature = "emit_state_diff")]
                state_before,
                ask_order: if ask_order_is_new { Some(ask_order_before) } else { None },
                bid_order: if bid_order_is_new { Some(bid_order_before) } else { None },
                ..trade
            };
            persistor.put_trade(&trade);
            //}
            maker.frozen -= if maker_is_bid { traded_quote_amount } else { traded_base_amount };

            // 检查maker是否完全成交
            let maker_finished = maker.remain.is_zero();
            if maker_finished {
                finished_orders.push(*maker);
            } else {
                // When maker_finished, `order_finish` will send message.
                // So we don't need to send the finish message here.
                persistor.put_order(&maker, OrderEventType::UPDATE);
            }

            // Save this trade price to market.
            // 更新市场最新价格
            self.price = price;
        }

        // 处理已完成的订单
        for item in finished_orders.iter() {
            self.order_finish(&mut *balance_manager, persistor, item);
        }

        // 处理taker订单的最终状态
        if need_cancel {
            // Now both self trade orders and immediately triggered post_only
            // limit orders will be cancelled here.
            // TODO: use CANCEL event here
            // 需要取消的订单(自成交或post_only触发)
            persistor.put_order(&taker, OrderEventType::FINISH);
        } else if taker.type_ == OrderType::MARKET {
            // market order can either filled or not
            // if it is filled, `FINISH` is ok
            // if it is not filled, `CANCELED` may be a better choice?
            // 市价单完成
            persistor.put_order(&taker, OrderEventType::FINISH);
        } else {
            // now the order type is limit
            // 限价单处理
            if taker.remain.is_zero() {
                // 完全成交
                persistor.put_order(&taker, OrderEventType::FINISH);
            } else {
                // `insert_order` will update the order info
                // 部分成交或未成交,插入订单簿
                taker = self.insert_order_into_orderbook(taker);
                self.frozen_balance(balance_manager, &taker);
            }
        }

        log::debug!("execute_order done {:?}", taker);
        taker // 返回处理后的taker订单
    }

    // 将订单插入订单簿
    pub fn insert_order_into_orderbook(&mut self, mut order: Order) -> Order {
        // 计算需要冻结的金额
        // 如果是卖单(ASK),冻结的是基础货币数量
        // 如果是买单(BID),冻结的是报价货币数量(数量*价格)
        if order.side == OrderSide::ASK {
            order.frozen = order.remain; // 卖单冻结剩余数量
        } else {
            order.frozen = order.remain * order.price; // 买单冻结剩余成交金额 (剩余数量 * 价格)
        }
        debug_assert_eq!(order.type_, OrderType::LIMIT);
        debug_assert!(!self.orders.contains_key(&order.id));
        // log::debug!("order insert {}", &order.id);
        let order_rc = OrderRc::new(order);
        // 将订单添加到全局订单映射中，borrow 是读锁 获取订单的引用
        let order = order_rc.borrow();
        self.orders.insert(order.id, order_rc.clone());

        // 将订单添加到用户订单映射中
        // 如果用户没有订单映射则创建新的
        let user_map = self.users.entry(order.user).or_insert_with(BTreeMap::new);
        debug_assert!(!user_map.contains_key(&order.id)); // 确保用户订单映射中不存在该订单
        user_map.insert(order.id, order_rc.clone());

        // 根据订单类型(买/卖)将订单添加到相应的订单队列中
        if order.side == OrderSide::ASK {
            // 卖单:添加到卖单队列(asks)
            let key = order.get_ask_key();
            debug_assert!(!self.asks.contains_key(&key)); // 确保卖单队列中不存在该订单
            self.asks.insert(key, order_rc.clone());
        } else {
            // 买单:添加到买单队列(bids)
            let key = order.get_bid_key();
            debug_assert!(!self.bids.contains_key(&key)); // 确保买单队列中不存在该订单
            self.bids.insert(key, order_rc.clone());
        }

        // 返回订单的深拷贝
        order_rc.deep()
    }

    // 完成订单处理函数
    // 当订单完全成交或被取消时调用此函数来清理订单相关的数据结构
    fn order_finish(&mut self, balance_manager: &mut BalanceManagerWrapper<'_>, persistor: &mut impl PersistExector, order: &Order) {
        // 根据订单类型(买/卖)从相应的订单簿中移除订单
        if order.side == OrderSide::ASK {
            // 如果是卖单,从卖单队列中移除
            let key = &order.get_ask_key();
            debug_assert!(self.asks.contains_key(key)); // 确保订单存在于卖单队列中
            self.asks.remove(key);
        } else {
            // 如果是买单,从买单队列中移除
            let key = &order.get_bid_key();
            debug_assert!(self.bids.contains_key(key)); // 确保订单存在于买单队列中
            self.bids.remove(key);
        }

        // 解冻与订单相关的用户余额
        self.unfrozen_balance(balance_manager, order);

        // 从全局订单映射中移除订单
        debug_assert!(self.orders.contains_key(&order.id)); // 确保订单存在于全局订单映射中
        self.orders.remove(&order.id);

        // 从用户订单映射中移除订单
        let user_map = self.users.get_mut(&order.user).unwrap();
        debug_assert!(user_map.contains_key(&order.id)); // 确保订单存在于用户订单映射中
        user_map.remove(&order.id);

        // 持久化订单完成事件
        persistor.put_order(order, OrderEventType::FINISH);
    }

    // for debugging
    fn get_trade_state(
        ask: &Order,
        bid: &Order,
        balance_manager: &mut BalanceManagerWrapper<'_>,
        base: &'static str,
        quote: &'static str,
    ) -> VerboseTradeState {
        let ask_order_state = VerboseOrderState {
            user_id: ask.user,
            order_id: ask.id,
            order_side: ask.side,
            finished_base: ask.finished_base,
            finished_quote: ask.finished_quote,
            finished_fee: ask.finished_fee,
        };
        let bid_order_state = VerboseOrderState {
            user_id: bid.user,
            order_id: bid.id,
            order_side: bid.side,
            finished_base: bid.finished_base,
            finished_quote: bid.finished_quote,
            finished_fee: bid.finished_fee,
        };
        let ask_user_base = balance_manager.balance_total(ask.user, base);
        let ask_user_quote = balance_manager.balance_total(ask.user, quote);
        let bid_user_base = balance_manager.balance_total(bid.user, base);
        let bid_user_quote = balance_manager.balance_total(bid.user, quote);
        VerboseTradeState {
            order_states: vec![ask_order_state, bid_order_state],
            balance_states: vec![
                VerboseBalanceState {
                    user_id: ask.user,
                    asset: base.into(),
                    balance: ask_user_base,
                },
                VerboseBalanceState {
                    user_id: ask.user,
                    asset: quote.into(),
                    balance: ask_user_quote,
                },
                VerboseBalanceState {
                    user_id: bid.user,
                    asset: base.into(),
                    balance: bid_user_base,
                },
                VerboseBalanceState {
                    user_id: bid.user,
                    asset: quote.into(),
                    balance: bid_user_quote,
                },
            ],
        }
    }
    // 取消单个订单
    pub fn cancel(&mut self, mut balance_manager: BalanceManagerWrapper<'_>, persistor: &mut impl PersistExector, order_id: u64) -> Order {
        let order = self.orders.get(&order_id).unwrap();
        let order_struct = order.deep();
        self.order_finish(&mut balance_manager, persistor, &order_struct);
        order_struct
    }
    // 取消用户所有订单
    pub fn cancel_all_for_user(
        &mut self,
        mut balance_manager: BalanceManagerWrapper<'_>,
        persistor: &mut impl PersistExector,
        user_id: u32,
    ) -> usize {
        // TODO: can we mutate while iterate?
        let order_ids: Vec<u64> = self.users.get(&user_id).unwrap_or(&BTreeMap::new()).keys().copied().collect();
        let total = order_ids.len();
        for order_id in order_ids {
            let order = self.orders.get(&order_id).unwrap();
            let order_struct = order.deep();
            self.order_finish(&mut balance_manager, persistor, &order_struct);
        }
        total
    }
    // 获取订单信息
    pub fn get(&self, order_id: u64) -> Option<Order> {
        self.orders.get(&order_id).map(OrderRc::deep)
    }
    pub fn get_order_num_of_user(&self, user_id: u32) -> usize {
        self.users.get(&user_id).map(|m| m.len()).unwrap_or(0)
    }
    pub fn get_order_of_user(&self, user_id: u32) -> Vec<Order> {
        self.users
            .get(&user_id)
            .unwrap_or(&BTreeMap::new())
            .values()
            .map(OrderRc::deep)
            .collect()
    }
    pub fn print(&self) {
        log::info!("orders:");
        for (k, v) in self.orders.iter() {
            log::info!("{}, {:?}", k, v.borrow())
        }
    }
    // 获取市场状态
    pub fn status(&self) -> MarketStatus {
        MarketStatus {
            name: self.name.to_string(),
            ask_count: self.asks.len(),
            ask_amount: self.asks.values().map(|item| item.borrow().remain).sum(),
            bid_count: self.bids.len(),
            bid_amount: self.bids.values().map(|item| item.borrow().remain).sum(),
            trade_count: self.trade_count,
        }
    }
    // 获取市场深度
    pub fn depth(&self, limit: usize, interval: &Decimal) -> MarketDepth {
        if interval.is_zero() {
            let id_fn = |order: &Order| -> Decimal { order.price };
            MarketDepth {
                asks: Self::group_ordebook_by_fn(&self.asks, limit, id_fn),
                bids: Self::group_ordebook_by_fn(&self.bids, limit, id_fn),
            }
        } else {
            let ask_group_fn = |order: &Order| -> Decimal { (order.price / interval).ceil() * interval };
            let bid_group_fn = |order: &Order| -> Decimal { (order.price / interval).floor() * interval };
            MarketDepth {
                asks: Self::group_ordebook_by_fn(&self.asks, limit, ask_group_fn),
                bids: Self::group_ordebook_by_fn(&self.bids, limit, bid_group_fn),
            }
        }
    }

    fn group_ordebook_by_fn<K, F>(orderbook: &BTreeMap<K, OrderRc>, limit: usize, f: F) -> Vec<PriceInfo>
    where
        F: Fn(&Order) -> Decimal,
    {
        orderbook
            .values()
            .group_by(|order_rc| -> Decimal { f(&order_rc.borrow()) })
            .into_iter()
            .take(limit)
            .map(|(price, group)| PriceInfo {
                price,
                amount: group.map(|order_rc| order_rc.borrow().remain).sum(),
            })
            .collect::<Vec<PriceInfo>>()
    }
}

pub struct MarketStatus {
    pub name: String,
    pub ask_count: usize,
    pub ask_amount: Decimal,
    pub bid_count: usize,
    pub bid_amount: Decimal,
    pub trade_count: u64,
}

pub struct PriceInfo {
    pub price: Decimal,
    pub amount: Decimal,
}

pub struct MarketDepth {
    pub asks: Vec<PriceInfo>,
    pub bids: Vec<PriceInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BalanceHistoryFromTrade {
    pub market: String,
    pub order_id: u64,
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Serialize, Deserialize, Debug)]
struct BalanceHistoryFromFee {
    pub market: String,
    pub order_id: u64,
    pub price: Decimal,
    pub amount: Decimal,
    pub fee_rate: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::update_controller::{BalanceUpdateParams, BusinessType};
    use crate::config::Settings;
    use crate::matchengine::mock;
    use crate::message::{Message, OrderMessage};
    use fluidex_common::rust_decimal_macros::*;
    use mock::*;

    //#[cfg(feature = "emit_state_diff")]
    #[test]
    fn test_multi_orders() {
        use crate::asset::BalanceUpdateController;
        use crate::matchengine::market::{Market, OrderInput};
        use crate::types::{OrderSide, OrderType};
        use fluidex_common::rust_decimal::prelude::FromPrimitive;
        use rand::Rng;

        let only_int = true;
        let broker = std::env::var("KAFKA_BROKER");
        let mut persistor: Box<dyn PersistExector> = match broker {
            Ok(b) => Box::new(crate::persist::MessengerBasedPersistor::new(Box::new(
                crate::message::FullOrderMessageManager::new_and_run(&b).unwrap(),
            ))),
            Err(_) => Box::new(crate::persist::FileBasedPersistor::new("market_test_output.txt")),
        };
        //let persistor = &mut persistor;
        let mut update_controller = BalanceUpdateController::new();
        let balance_manager = &mut get_simple_balance_manager(get_simple_asset_config(if only_int { 0 } else { 6 }));
        let uid0 = 0;
        let uid1 = 1;
        let mut update_balance_fn = |seq_id, user_id, asset: &str, amount| {
            update_controller
                .update_user_balance(
                    balance_manager,
                    &mut persistor,
                    BalanceUpdateParams {
                        balance_type: BalanceType::AVAILABLE,
                        business_type: BusinessType::Deposit,
                        user_id,
                        asset: asset.to_string(),
                        business: "deposit".to_owned(),
                        business_id: seq_id,
                        market_price: Decimal::zero(),
                        change: amount,
                        detail: serde_json::Value::default(),
                        signature: vec![],
                    },
                )
                .unwrap();
        };
        update_balance_fn(0, uid0, &MockAsset::USDT.id(), dec!(1_000_000));
        update_balance_fn(1, uid0, &MockAsset::ETH.id(), dec!(1_000_000));
        update_balance_fn(2, uid1, &MockAsset::USDT.id(), dec!(1_000_000));
        update_balance_fn(3, uid1, &MockAsset::ETH.id(), dec!(1_000_000));

        let sequencer = &mut Sequencer::default();
        let market_conf = if only_int {
            mock::get_integer_prec_market_config()
        } else {
            mock::get_simple_market_config()
        };
        let mut market = Market::new(&market_conf, &Settings::default(), balance_manager).unwrap();
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let user_id = if rng.gen::<bool>() { uid0 } else { uid1 };
            let side = if rng.gen::<bool>() { OrderSide::BID } else { OrderSide::ASK };
            let amount = if only_int {
                Decimal::from_i32(rng.gen_range(1..10)).unwrap()
            } else {
                Decimal::from_f64(rng.gen_range(1.0..10.0)).unwrap()
            };
            let price = if only_int {
                Decimal::from_i32(rng.gen_range(120..140)).unwrap()
            } else {
                Decimal::from_f64(rng.gen_range(120.0..140.0)).unwrap()
            };
            let order = OrderInput {
                user_id,
                side,
                type_: OrderType::LIMIT,
                // the matchengine will truncate precision
                // but later we'd better truncate precision outside
                amount,
                price,
                quote_limit: dec!(0),
                taker_fee: dec!(0),
                maker_fee: dec!(0),
                market: market.name.to_string(),
                post_only: false,
                signature: [0; 64],
            };
            market
                .put_order(sequencer, balance_manager.into(), &mut update_controller, &mut persistor, order)
                .unwrap();
        }
    }
    #[test]
    fn test_market_taker_is_bid() {
        let mut update_controller = BalanceUpdateController::new();
        let balance_manager = &mut get_simple_balance_manager(get_simple_asset_config(8));

        balance_manager.add(101, BalanceType::AVAILABLE, &MockAsset::USDT.id(), &dec!(300));
        balance_manager.add(102, BalanceType::AVAILABLE, &MockAsset::USDT.id(), &dec!(300));
        balance_manager.add(101, BalanceType::AVAILABLE, &MockAsset::ETH.id(), &dec!(1000));
        balance_manager.add(102, BalanceType::AVAILABLE, &MockAsset::ETH.id(), &dec!(1000));

        let sequencer = &mut Sequencer::default();
        let mut persistor = crate::persist::DummyPersistor::default();
        let ask_user_id = 101;
        let mut market = Market::new(&get_simple_market_config(), &Settings::default(), balance_manager).unwrap();
        let ask_order_input = OrderInput {
            user_id: ask_user_id,
            side: OrderSide::ASK,
            type_: OrderType::LIMIT,
            amount: dec!(20.0),
            price: dec!(0.1),
            quote_limit: dec!(0),
            taker_fee: dec!(0.001),
            maker_fee: dec!(0.001),
            market: market.name.to_string(),
            post_only: false,
            signature: [0; 64],
        };
        let ask_order = market
            .put_order(
                sequencer,
                balance_manager.into(),
                &mut update_controller,
                &mut persistor,
                ask_order_input,
            )
            .unwrap();
        assert_eq!(ask_order.id, 1);
        assert_eq!(ask_order.remain, dec!(20.0));

        let bid_user_id = 102;
        let bid_order_input = OrderInput {
            user_id: bid_user_id,
            side: OrderSide::BID,
            type_: OrderType::MARKET,
            amount: dec!(10.0),
            price: dec!(0),
            quote_limit: dec!(0),
            taker_fee: dec!(0.001),
            maker_fee: dec!(0.001),
            market: market.name.to_string(),
            post_only: false,
            signature: [0; 64],
        };
        let bid_order = market
            .put_order(
                sequencer,
                balance_manager.into(),
                &mut update_controller,
                &mut persistor,
                bid_order_input,
            )
            .unwrap();
        // trade: price: 0.10 amount: 10
        assert_eq!(bid_order.id, 2);
        assert_eq!(bid_order.remain, dec!(0));
        assert_eq!(bid_order.finished_quote, dec!(1));
        assert_eq!(bid_order.finished_base, dec!(10));
        assert_eq!(bid_order.finished_fee, dec!(0.01));

        //market.print();

        let ask_order = market.get(ask_order.id).unwrap();
        assert_eq!(ask_order.remain, dec!(10));
        assert_eq!(ask_order.finished_quote, dec!(1));
        assert_eq!(ask_order.finished_base, dec!(10));
        assert_eq!(ask_order.finished_fee, dec!(0.001));

        // original balance: btc 300, eth 1000
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::AVAILABLE, &MockAsset::ETH.id()),
            dec!(980)
        );
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::FREEZE, &MockAsset::ETH.id()),
            dec!(10)
        );

        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::AVAILABLE, &MockAsset::USDT.id()),
            dec!(300.999)
        );
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::FREEZE, &MockAsset::USDT.id()),
            dec!(0)
        );

        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::AVAILABLE, &MockAsset::ETH.id()),
            dec!(1009.99)
        );
        assert_eq!(balance_manager.get(bid_user_id, BalanceType::FREEZE, &MockAsset::ETH.id()), dec!(0));

        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::AVAILABLE, &MockAsset::USDT.id()),
            dec!(299)
        );
        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::FREEZE, &MockAsset::USDT.id()),
            dec!(0)
        );

        //assert_eq!(persistor.orders.len(), 3);
        //assert_eq!(persistor.trades.len(), 1);
    }

    #[test]
    fn test_limit_post_only_orders() {
        let mut update_controller = BalanceUpdateController::new();
        let balance_manager = &mut get_simple_balance_manager(get_simple_asset_config(8));

        balance_manager.add(201, BalanceType::AVAILABLE, &MockAsset::USDT.id(), &dec!(300));
        balance_manager.add(202, BalanceType::AVAILABLE, &MockAsset::USDT.id(), &dec!(300));
        balance_manager.add(201, BalanceType::AVAILABLE, &MockAsset::ETH.id(), &dec!(1000));
        balance_manager.add(202, BalanceType::AVAILABLE, &MockAsset::ETH.id(), &dec!(1000));

        let sequencer = &mut Sequencer::default();
        let mut persistor = crate::persist::MemBasedPersistor::default();
        let ask_user_id = 201;
        let mut market = Market::new(&get_simple_market_config(), &Settings::default(), balance_manager).unwrap();
        let ask_order_input = OrderInput {
            user_id: ask_user_id,
            side: OrderSide::ASK,
            type_: OrderType::LIMIT,
            amount: dec!(20.0),
            price: dec!(0.1),
            quote_limit: dec!(0),
            taker_fee: dec!(0.001),
            maker_fee: dec!(0.001),
            market: market.name.to_string(),
            post_only: true,
            signature: [0; 64],
        };
        let ask_order = market
            .put_order(
                sequencer,
                balance_manager.into(),
                &mut update_controller,
                &mut persistor,
                ask_order_input,
            )
            .unwrap();

        assert_eq!(ask_order.id, 1);
        assert_eq!(ask_order.remain, dec!(20));

        let bid_user_id = 202;
        let bid_order_input = OrderInput {
            user_id: bid_user_id,
            side: OrderSide::BID,
            type_: OrderType::LIMIT,
            amount: dec!(10.0),
            price: dec!(0.1),
            quote_limit: dec!(0),
            taker_fee: dec!(0.001),
            maker_fee: dec!(0.001),
            market: market.name.to_string(),
            post_only: true,
            signature: [0; 64],
        };
        let bid_order = market
            .put_order(
                sequencer,
                balance_manager.into(),
                &mut update_controller,
                &mut persistor,
                bid_order_input,
            )
            .unwrap();

        // No trade occurred since limit and post only. This BID order should be finished.
        assert_eq!(bid_order.id, 2);
        assert_eq!(bid_order.remain, dec!(10));
        assert_eq!(bid_order.finished_quote, dec!(0));
        assert_eq!(bid_order.finished_base, dec!(0));
        assert_eq!(bid_order.finished_fee, dec!(0));

        let ask_order = market.get(ask_order.id).unwrap();
        assert_eq!(ask_order.remain, dec!(20));
        assert_eq!(ask_order.finished_quote, dec!(0));
        assert_eq!(ask_order.finished_base, dec!(0));
        assert_eq!(ask_order.finished_fee, dec!(0));

        let bid_order_message = persistor.messages.last().unwrap();
        match bid_order_message {
            Message::OrderMessage(msg) => {
                assert!(matches!(
                    **msg,
                    OrderMessage {
                        event: OrderEventType::FINISH,
                        order: Order { id: 2, user: 202, .. },
                        ..
                    }
                ));
            }
            _ => panic!("expect OrderMessage only"),
        }

        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::AVAILABLE, &MockAsset::ETH.id()),
            dec!(980)
        );
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::FREEZE, &MockAsset::ETH.id()),
            dec!(20)
        );
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::AVAILABLE, &MockAsset::USDT.id()),
            dec!(300)
        );
        assert_eq!(
            balance_manager.get(ask_user_id, BalanceType::FREEZE, &MockAsset::USDT.id()),
            dec!(0)
        );

        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::AVAILABLE, &MockAsset::ETH.id()),
            dec!(1000)
        );
        assert_eq!(balance_manager.get(bid_user_id, BalanceType::FREEZE, &MockAsset::ETH.id()), dec!(0));
        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::AVAILABLE, &MockAsset::USDT.id()),
            dec!(300)
        );
        assert_eq!(
            balance_manager.get(bid_user_id, BalanceType::FREEZE, &MockAsset::USDT.id()),
            dec!(0)
        );
    }
}

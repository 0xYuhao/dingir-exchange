use crate::types::{OrderSide, OrderType};
use crate::utils::InternedString;
use fluidex_common::types::{BigInt, Decimal, Fr, FrExt};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct MarketKeyAsk {
    pub order_price: Decimal,
    pub order_id: u64,
}

#[derive(PartialEq, Eq)]
pub struct MarketKeyBid {
    pub order_price: Decimal,
    pub order_id: u64,
}

impl Ord for MarketKeyBid {
    fn cmp(&self, other: &Self) -> Ordering {
        let price_order = self.order_price.cmp(&other.order_price).reverse();
        if price_order != Ordering::Equal {
            price_order
        } else {
            self.order_id.cmp(&other.order_id)
        }
    }
}

#[cfg(test)]
#[test]
fn test_order_sort() {
    use fluidex_common::rust_decimal::prelude::One;
    use fluidex_common::rust_decimal::prelude::Zero;
    {
        let o1 = MarketKeyBid {
            order_price: Decimal::zero(),
            order_id: 5,
        };
        let o2 = MarketKeyBid {
            order_price: Decimal::zero(),
            order_id: 6,
        };
        let o3 = MarketKeyBid {
            order_price: Decimal::one(),
            order_id: 7,
        };
        assert!(o1 < o2);
        assert!(o3 < o2);
    }
    {
        let o1 = MarketKeyAsk {
            order_price: Decimal::zero(),
            order_id: 5,
        };
        let o2 = MarketKeyAsk {
            order_price: Decimal::zero(),
            order_id: 6,
        };
        let o3 = MarketKeyAsk {
            order_price: Decimal::one(),
            order_id: 7,
        };
        assert!(o1 < o2);
        assert!(o3 > o2);
    }
}

impl PartialOrd for MarketKeyBid {
    fn partial_cmp(&self, other: &MarketKeyBid) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Order {
    // Order can be seen as two part:
    // first, const part, these fields cannot be updated
    // then, the updatable part, which changes whenever a trade occurs
    // Order 结构可以分为两部分:
    // 第一部分是常量部分，这些字段创建后不可更改
    // 第二部分是可变部分，会随着交易的发生而更新
    // === 常量部分 ===
    pub id: u64,                // 订单唯一标识符
    pub base: InternedString,   // 基础货币符号
    pub quote: InternedString,  // 报价货币符号
    pub market: InternedString, // 交易市场标识
    #[serde(rename = "type")]
    pub type_: OrderType, // 订单类型(如:限价单、市价单等)
    pub side: OrderSide,        // 订单方向(买入/卖出)
    pub user: u32,              // 用户ID
    pub post_only: bool,        // 是否仅作为挂单(maker)
    #[serde(with = "crate::utils::serde::HexArray")]
    pub signature: [u8; 64], // 订单签名
    pub price: Decimal,         // 订单价格
    pub amount: Decimal,        // 订单总数量
    pub maker_fee: Decimal,     // 作为maker时的手续费率
    pub taker_fee: Decimal,     // 作为taker时的手续费率(post_only为true时无用)
    pub create_time: f64,       // 订单创建时间

    // below are the changable parts
    // === 可变部分 ===
    // remain + finished_base == amount
    pub remain: Decimal, // 剩余未成交数量(remain + finished_base = amount)
    // frozen = if ask { amount (base) } else { amount * price (quote) }
    pub frozen: Decimal,         // 冻结金额(卖单时为base货币数量，买单时为quote货币数量 = amount * price)
    pub finished_base: Decimal,  // 已成交的基础货币数量
    pub finished_quote: Decimal, // 已成交的计价货币数量
    pub finished_fee: Decimal,   // 已产生的手续费
    pub update_time: f64,        // 最后更新时间
}

/*
fn de_market_string<'de, D: serde::de::Deserializer<'de>>(_deserializer: D) -> Result<&'static str, D::Error> {
    Ok("Test")
}
*/

impl Order {
    pub fn get_ask_key(&self) -> MarketKeyAsk {
        MarketKeyAsk {
            order_price: self.price,
            order_id: self.id,
        }
    }
    pub fn get_bid_key(&self) -> MarketKeyBid {
        MarketKeyBid {
            order_price: self.price,
            order_id: self.id,
        }
    }
    pub fn is_ask(&self) -> bool {
        self.side == OrderSide::ASK
    }
}

#[derive(Clone, Debug)]
pub struct OrderRc(Arc<RwLock<Order>>);

/*
    simulate behavior like RefCell, the syncing is ensured by locking in higher rank
    here we use RwLock only for avoiding unsafe tag, we can just use raw pointer
    casted from ARc rather than RwLock here if we do not care about unsafe
*/
impl OrderRc {
    pub(super) fn new(order: Order) -> Self {
        OrderRc(Arc::new(RwLock::new(order)))
    }

    pub fn borrow(&self) -> RwLockReadGuard<'_, Order> {
        self.0.try_read().expect("Lock for parent entry ensure it")
    }

    pub(super) fn borrow_mut(&mut self) -> RwLockWriteGuard<'_, Order> {
        self.0.try_write().expect("Lock for parent entry ensure it")
    }

    pub fn deep(&self) -> Order {
        *self.borrow()
    }
}

pub struct OrderInput {
    pub user_id: u32,
    pub side: OrderSide,
    pub type_: OrderType,
    pub amount: Decimal,
    pub price: Decimal,
    pub quote_limit: Decimal,
    pub taker_fee: Decimal, // FIXME fee should be determined inside engine rather than take from input
    pub maker_fee: Decimal,
    pub market: String,
    pub post_only: bool,
    pub signature: [u8; 64],
}

pub struct OrderCommitment {
    // order_id
    // account_id
    // nonce
    pub token_sell: Fr,
    pub token_buy: Fr,
    pub total_sell: Fr,
    pub total_buy: Fr,
}

impl OrderCommitment {
    pub fn hash(&self) -> BigInt {
        // consistent with https://github.com/fluidex/circuits/blob/d6e06e964b9d492f1fa5513bcc2295e7081c540d/helper.ts/state-utils.ts#L38
        // TxType::PlaceOrder
        let magic_head = Fr::from_u32(4);
        let data = Fr::hash(&[
            magic_head,
            // TODO: sign nonce or order_id
            //u32_to_fr(self.order_id),
            self.token_sell,
            self.token_buy,
            self.total_sell,
            self.total_buy,
        ]);
        //data = hash([data, accountID, nonce]);
        // nonce and orderID seems redundant?

        // account_id is not needed if the hash is signed later?
        //data = hash(&[data, u32_to_fr(self.account_id)]);
        data.to_bigint()
    }
}

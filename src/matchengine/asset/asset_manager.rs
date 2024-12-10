use crate::config;
use crate::market::{Market, OrderCommitment};
use anyhow::{bail, Result};
use fluidex_common::rust_decimal::{self, RoundingStrategy};
use fluidex_common::types::{DecimalExt, FrExt};
use fluidex_common::Fr;
use orchestra::rpc::exchange::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub struct AssetInfo {
    pub prec_save: u32, // 保存精度
    pub prec_show: u32, // 显示精度
    pub inner_id: u32,  // 内部ID
}

#[derive(Clone)]
// 资产管理器
pub struct AssetManager {
    pub assets: HashMap<String, AssetInfo>, // 资产映射表
}

impl AssetManager {
    // 创建资产管理器
    pub fn new(asset_config: &[config::Asset]) -> Result<AssetManager> {
        log::info!("asset {:?}", asset_config);
        let mut assets = HashMap::new();
        // 遍历资产配置
        for item in asset_config.iter() {
            // 将资产配置插入到资产映射表中
            assets.insert(
                item.id.clone(),
                AssetInfo {
                    prec_save: item.prec_save,
                    prec_show: item.prec_show,
                    inner_id: item.rollup_token_id as u32,
                },
            );
        }
        Ok(AssetManager { assets })
    }

    // 添加资产
    pub fn append(&mut self, asset_config: &[config::Asset]) {
        //log::info()
        for item in asset_config.iter() {
            let ret = self.assets.insert(
                item.id.clone(),
                AssetInfo {
                    prec_save: item.prec_save,
                    prec_show: item.prec_show,
                    inner_id: item.rollup_token_id as u32,
                },
            );
            // 如果插入成功，则打印更新资产信息
            if ret.is_some() {
                log::info!("Update asset {}", item.id);
            } else {
                // 如果插入失败，则打印添加新资产信息
                log::info!("Append new asset {}", item.id);
            }
        }
    }

    // 检查资产是否存在
    pub fn asset_exist(&self, id: &str) -> bool {
        self.assets.contains_key(id)
    }

    // 获取资产信息
    pub fn asset_get(&self, id: &str) -> Option<&AssetInfo> {
        self.assets.get(id)
    }
    // 获取资产保存精度
    pub fn asset_prec(&self, id: &str) -> u32 {
        self.asset_get(id).unwrap().prec_save
    }
    // 获取资产显示精度
    pub fn asset_prec_show(&self, id: &str) -> u32 {
        self.asset_get(id).unwrap().prec_show
    }
    // 提交订单,生成订单对象
    pub fn commit_order(&self, o: &OrderPutRequest, market: &Market) -> Result<OrderCommitment> {
        // 将市场ID拆分为基础资产和报价资产
        let assets: Vec<&str> = o.market.split('_').collect();
        // 检查市场ID是否包含两个资产
        if assets.len() != 2 {
            bail!("market error");
        }
        // 获取基础资产信息
        let base_token = match self.asset_get(assets[0]) {
            Some(token) => token,
            None => bail!("market base_token error"),
        };
        // 获取报价资产信息
        let quote_token = match self.asset_get(assets[1]) {
            Some(token) => token,
            None => bail!("market quote_token error"),
        };
        // 将订单数量四舍五入到市场数量精度
        let amount = match rust_decimal::Decimal::from_str(&o.amount) {
            // market.amount_prec 指定了要保留的小数位数
            // 向零舍入意味着无论是正数还是负数，都会向0的方向舍入（即截断）
            Ok(d) => d.round_dp_with_strategy(market.amount_prec, RoundingStrategy::ToZero),
            _ => bail!("amount error"),
        };
        // 将订单价格四舍五入到市场价格精度
        let price = match rust_decimal::Decimal::from_str(&o.price) {
            Ok(d) => d.round_dp(market.price_prec),
            _ => bail!("price error"),
        };

        // 根据订单方向生成订单对象
        match OrderSide::from_i32(o.order_side) {
            // 如果订单方向为卖单
            Some(OrderSide::Ask) => Ok(OrderCommitment {
                // 买入报价资产 （quote asset）
                // to_fr是一个自定义的trait方法（通过代码中的use fluidex_common::types::DecimalExt可以看出），它的主要作用是将十进制数（Decimal）转换为有限域元素（Field Element，简写为Fr）适用于零知识证明 我们应该不需要
                token_buy: Fr::from_u32(quote_token.inner_id),
                // 卖出基础资产 （base asset）
                token_sell: Fr::from_u32(base_token.inner_id),
                // 买入的报价资产金额(amount * price)
                total_buy: (amount * price).to_fr(market.amount_prec + market.price_prec),
                // 卖出的基础资产数量
                total_sell: amount.to_fr(market.amount_prec),
            }),
            // 如果订单方向为买单
            Some(OrderSide::Bid) => Ok(OrderCommitment {
                // 买入基础资产(base asset)
                token_buy: Fr::from_u32(base_token.inner_id),
                // 卖出报价资产(quote asset)
                token_sell: Fr::from_u32(quote_token.inner_id),
                // 买入基础资产的数量
                total_buy: amount.to_fr(market.amount_prec),
                // 卖出报价资产的金额(amount * price)
                total_sell: (amount * price).to_fr(market.amount_prec + market.price_prec),
            }),
            // 如果订单方向无效
            None => bail!("market error"),
        }
    }
}

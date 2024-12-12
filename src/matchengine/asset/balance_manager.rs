use super::asset_manager::AssetManager;
use crate::config;
pub use crate::models::BalanceHistory;

use anyhow::Result;
use fluidex_common::rust_decimal::prelude::Zero;
use fluidex_common::rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use num_enum::TryFromPrimitive;
use std::collections::HashMap;

// 余额类型枚举 - 定义了两种余额状态
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash, Copy, TryFromPrimitive)]
#[repr(i16)]
pub enum BalanceType {
    AVAILABLE = 1, // 可用余额
    FREEZE = 2,    // 冻结余额
}

// 余额映射键结构体 - 用于唯一标识一个用户的某个资产余额
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub struct BalanceMapKey {
    pub user_id: u32,              // 用户ID
    pub balance_type: BalanceType, // 余额类型
    pub asset: String,             // 资产名称
}

// 余额状态结构体 - 用于统计资产的总体状态
#[derive(Default)]
pub struct BalanceStatus {
    pub total: Decimal,       // 总额
    pub available_count: u32, // 可用余额计数
    pub available: Decimal,   // 可用总额
    pub frozen_count: u32,    // 冻结余额计数
    pub frozen: Decimal,      // 冻结总额
}

// 余额管理器结构体
pub struct BalanceManager {
    pub asset_manager: AssetManager,               // 资产管理器实例
    pub balances: HashMap<BalanceMapKey, Decimal>, // 余额映射表
}

impl BalanceManager {
    // 创建新的余额管理器实例
    pub fn new(asset_config: &[config::Asset]) -> Result<BalanceManager> {
        let asset_manager = AssetManager::new(asset_config)?;
        Ok(BalanceManager {
            asset_manager,
            balances: HashMap::new(),
        })
    }

    // 重置所有余额
    pub fn reset(&mut self) {
        self.balances.clear()
    }

    // 获取指定用户的指定资产余额
    pub fn get(&self, user_id: u32, balance_type: BalanceType, asset: &str) -> Decimal {
        self.get_by_key(&BalanceMapKey {
            user_id,
            balance_type,
            asset: asset.to_owned(), // to_owned() 将 &str 转换为 String 转换所有权
        })
    }

    // 获取经过精度舍入的余额
    pub fn get_with_round(&self, user_id: u32, balance_type: BalanceType, asset: &str) -> Decimal {
        let balance: Decimal = self.get(user_id, balance_type, asset);
        let prec_save = self.asset_manager.asset_prec(asset);
        let prec_show = self.asset_manager.asset_prec_show(asset);
        let balance_show = if prec_save == prec_show {
            balance
        } else {
            balance.round_dp(prec_show)
        };
        balance_show
    }

    // 根据键获取余额,如果不存在返回0
    pub fn get_by_key(&self, key: &BalanceMapKey) -> Decimal {
        *self.balances.get(key).unwrap_or(&Decimal::zero())
    }

    // 删除指定用户的指定资产余额
    pub fn del(&mut self, user_id: u32, balance_type: BalanceType, asset: &str) {
        self.balances.remove(&BalanceMapKey {
            user_id,
            balance_type,
            asset: asset.to_owned(),
        });
    }

    // 设置指定用户的指定资产余额
    pub fn set(&mut self, user_id: u32, balance_type: BalanceType, asset: &str, amount: &Decimal) {
        let key = BalanceMapKey {
            user_id,
            balance_type,
            asset: asset.to_owned(),
        };
        self.set_by_key(key, amount);
    }

    // 根据键设置余额
    pub fn set_by_key(&mut self, key: BalanceMapKey, amount: &Decimal) {
        // 检查 amount 是否为正数（大于或等于零） 只在debug模式生效
        debug_assert!(amount.is_sign_positive());
        let amount = amount.round_dp(self.asset_manager.asset_prec(&key.asset));
        //log::debug!("set balance: {:?}, {}", key, amount);
        self.balances.insert(key, amount);
    }

    // 增加指定用户的指定资产余额
    pub fn add(&mut self, user_id: u32, balance_type: BalanceType, asset: &str, amount: &Decimal) -> Decimal {
        debug_assert!(amount.is_sign_positive());
        let amount = amount.round_dp(self.asset_manager.asset_prec(asset));
        let key = BalanceMapKey {
            user_id,
            balance_type,
            asset: asset.to_owned(),
        };
        let old_value = self.get_by_key(&key);
        let new_value = old_value + amount;
        self.set_by_key(key, &new_value);
        new_value
    }

    // 减少指定用户的指定资产余额
    pub fn sub(&mut self, user_id: u32, balance_type: BalanceType, asset: &str, amount: &Decimal) -> Decimal {
        debug_assert!(amount.is_sign_positive());
        let amount = amount.round_dp(self.asset_manager.asset_prec(asset));
        let key = BalanceMapKey {
            user_id,
            balance_type,
            asset: asset.to_owned(),
        };
        let old_value = self.get_by_key(&key);
        debug_assert!(old_value.ge(&amount));
        let new_value = old_value - amount;
        debug_assert!(new_value.is_sign_positive());
        // TODO don't remove it. Skip when sql insert
        /*
        if result.is_zero() {
            self.balances.remove(&key);
        } else {
            self.balances.insert(key, result);
        }
        */
        self.set_by_key(key, &new_value);
        new_value
    }

    // 冻结指定用户的指定资产余额
    pub fn frozen(&mut self, user_id: u32, asset: &str, amount: &Decimal) {
        debug_assert!(amount.is_sign_positive());
        let amount = amount.round_dp(self.asset_manager.asset_prec(asset));
        let key = BalanceMapKey {
            user_id,
            balance_type: BalanceType::AVAILABLE,
            asset: asset.to_owned(),
        };
        let old_available_value = self.get_by_key(&key);
        debug_assert!(old_available_value.ge(&amount));
        self.sub(user_id, BalanceType::AVAILABLE, asset, &amount);
        self.add(user_id, BalanceType::FREEZE, asset, &amount);
    }

    // 解冻指定用户的指定资产余额
    pub fn unfrozen(&mut self, user_id: u32, asset: &str, amount: &Decimal) {
        debug_assert!(amount.is_sign_positive());
        let amount = amount.round_dp(self.asset_manager.asset_prec(asset));
        let key = BalanceMapKey {
            user_id,
            balance_type: BalanceType::FREEZE,
            asset: asset.to_owned(),
        };
        let old_frozen_value = self.get_by_key(&key);
        debug_assert!(
            old_frozen_value.ge(&amount),
            "unfreeze larger than frozen {} > {}",
            amount,
            old_frozen_value
        );
        self.add(user_id, BalanceType::AVAILABLE, asset, &amount);
        self.sub(user_id, BalanceType::FREEZE, asset, &amount);
    }

    // 获取指定用户的指定资产总余额(可用+冻结)
    pub fn total(&self, user_id: u32, asset: &str) -> Decimal {
        self.get(user_id, BalanceType::AVAILABLE, asset) + self.get(user_id, BalanceType::FREEZE, asset)
    }

    // 获取指定资产的总体状态统计
    pub fn status(&self, asset: &str) -> BalanceStatus {
        let mut result = BalanceStatus::default();
        for (k, amount) in self.balances.iter() {
            if k.asset.eq(asset) && !amount.is_zero() {
                result.total += amount;
                if k.balance_type == BalanceType::AVAILABLE {
                    result.available_count += 1;
                    result.available += amount;
                } else {
                    result.frozen_count += 1;
                    result.frozen += amount;
                }
            }
        }
        result
    }
}

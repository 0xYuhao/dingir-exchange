// 引入相关模块和依赖
use super::balance_manager::{BalanceManager, BalanceType};
use crate::models;
use crate::persist::PersistExector;
use fluidex_common::utils::timeutil::{current_timestamp, FTimestamp};
pub use models::BalanceHistory;

use anyhow::{bail, Result};
use fluidex_common::rust_decimal::Decimal;
use ttl_cache::TtlCache;

use std::time::Duration;

// 定义资产余额映射的初始大小
const BALANCE_MAP_INIT_SIZE_ASSET: usize = 64;
// 是否持久化零余额更新的标志
const PERSIST_ZERO_BALANCE_UPDATE: bool = false;

// 余额更新参数结构体,包含了一次余额更新所需的所有信息
pub struct BalanceUpdateParams {
    pub balance_type: BalanceType,   // 余额类型(可用/冻结)
    pub business_type: BusinessType, // 业务类型(存款/交易等)
    pub user_id: u32,                // 用户ID
    pub business_id: u64,            // 业务ID
    pub asset: String,               // 资产名称
    pub business: String,            // 业务描述
    pub market_price: Decimal,       // 市场价格
    pub change: Decimal,             // 变动金额
    pub detail: serde_json::Value,   // 详细信息(JSON格式)
    pub signature: Vec<u8>,          // 签名数据
}

// 业务类型枚举,定义了支持的业务类型
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum BusinessType {
    Deposit,  // 存款业务
    Trade,    // 交易业务
    Transfer, // 转账业务
    Withdraw, // 提现业务
}

// 余额更新键结构体,用于缓存中唯一标识一次余额更新
#[derive(PartialEq, Eq, Hash)]
struct BalanceUpdateKey {
    pub balance_type: BalanceType,   // 余额类型
    pub business_type: BusinessType, // 业务类型
    pub user_id: u32,                // 用户ID
    pub asset: String,               // 资产名称
    pub business: String,            // 业务描述
    pub business_id: u64,            // 业务ID
}

//pub trait BalanceUpdateValidator {
//    pub fn is_valid()
//}

// TODO: this class needs to be refactored
// Currently it has two purpose: (1) filter duplicate (2) generate message
//余额更新控制器
pub struct BalanceUpdateController {
    cache: TtlCache<BalanceUpdateKey, bool>, // 使用TTL缓存存储更新记录
}

// 余额更新控制器的实现
impl BalanceUpdateController {
    // 创建新的控制器实例
    pub fn new() -> BalanceUpdateController {
        let capacity = 1_000_000; // 缓存容量为100万条记录
        BalanceUpdateController {
            cache: TtlCache::new(capacity),
        }
    }

    // 重置缓存
    pub fn reset(&mut self) {
        self.cache.clear()
    }

    // 定时器触发时清理缓存
    pub fn on_timer(&mut self) {
        self.cache.clear()
    }

    // 获取定时器间隔时间(60秒)
    pub fn timer_interval(&self) -> Duration {
        Duration::from_secs(60)
    }
    // return false if duplicate
    pub fn update_user_balance(
        &mut self,
        balance_manager: &mut BalanceManager,
        persistor: &mut impl PersistExector,
        mut params: BalanceUpdateParams,
    ) -> Result<()> {
        // 解构参数
        let asset = params.asset;
        let balance_type = params.balance_type;
        let business = params.business;
        let business_type = params.business_type;
        let business_id = params.business_id;
        let user_id = params.user_id;

        // 构建缓存键
        let cache_key = BalanceUpdateKey {
            balance_type,
            business_type,
            user_id,
            asset: asset.clone(),
            business: business.clone(),
            business_id,
        };

        // 检查是否重复请求
        if self.cache.contains_key(&cache_key) {
            bail!("duplicate request");
        }

        // 获取当前余额
        let old_balance = balance_manager.get(user_id, balance_type, &asset);
        let change = params.change;
        let abs_change = change.abs();

        // 根据变动方向更新余额
        if change.is_sign_positive() {
            // 正数表示增加余额
            balance_manager.add(user_id, balance_type, &asset, &abs_change);
        } else if change.is_sign_negative() {
            // 负数表示减少余额,需要检查余额是否足够
            if old_balance < abs_change {
                bail!("balance not enough");
            }
            balance_manager.sub(user_id, balance_type, &asset, &abs_change);
        }

        // 记录余额变动日志
        log::debug!("change user balance: {} {} {}", user_id, asset, change);

        // 将更新记录加入缓存,有效期1小时
        self.cache.insert(cache_key, true, Duration::from_secs(3600));

        // 如果需要持久化且余额变动不为零,则保存历史记录
        if persistor.real_persist() && (PERSIST_ZERO_BALANCE_UPDATE || !change.is_zero()) {
            params.detail["id"] = serde_json::Value::from(business_id);

            // 获取最新的可用和冻结余额
            let balance_available = balance_manager.get(user_id, BalanceType::AVAILABLE, &asset);
            let balance_frozen = balance_manager.get(user_id, BalanceType::FREEZE, &asset);

            // 构建余额历史记录
            let balance_history = BalanceHistory {
                time: FTimestamp(current_timestamp()).into(),
                user_id: user_id as i32,
                business_id: business_id as i64,
                asset,
                business,
                market_price: params.market_price,
                change,
                balance: balance_available + balance_frozen,
                balance_available,
                balance_frozen,
                detail: params.detail.to_string(),
                signature: params.signature,
            };

            // 保存余额历史
            persistor.put_balance(&balance_history);

            // 对于存款和提现业务,额外保存相应记录
            match params.business_type {
                BusinessType::Deposit => persistor.put_deposit(&balance_history),
                BusinessType::Withdraw => persistor.put_withdraw(&balance_history),
                _ => {}
            }
        }

        Ok(())
    }
}

// 实现Default trait,使用new()作为默认构造方法
impl Default for BalanceUpdateController {
    fn default() -> Self {
        Self::new()
    }
}

/// 序列号生成器结构体
/// 用于生成订单、交易、消息和操作日志的唯一ID
#[derive(Default)]
pub struct Sequencer {
    order_id: u64,         // 订单ID序列号
    trade_id: u64,         // 交易ID序列号
    msg_id: u64,           // 消息ID序列号
    operation_log_id: u64, // 操作日志ID序列号
}

impl Sequencer {
    /// 重置所有序列号为0
    pub fn reset(&mut self) {
        self.set_operation_log_id(0);
        self.set_order_id(0);
        self.set_trade_id(0);
        self.set_msg_id(0);
    }

    /// 生成下一个订单ID
    pub fn next_order_id(&mut self) -> u64 {
        self.order_id += 1;
        //log::debug!("next_order_id {}", self.order_id);
        self.order_id
    }

    /// 生成下一个交易ID
    pub fn next_trade_id(&mut self) -> u64 {
        self.trade_id += 1;
        self.trade_id
    }

    /// 生成下一个操作日志ID
    pub fn next_operation_log_id(&mut self) -> u64 {
        self.operation_log_id += 1;
        self.operation_log_id
    }

    /// 生成下一个消息ID
    pub fn next_msg_id(&mut self) -> u64 {
        self.msg_id += 1;
        self.msg_id
    }

    /// 获取当前操作日志ID
    pub fn get_operation_log_id(&self) -> u64 {
        self.operation_log_id
    }

    /// 获取当前交易ID
    pub fn get_trade_id(&self) -> u64 {
        self.trade_id
    }

    /// 获取当前订单ID
    pub fn get_order_id(&self) -> u64 {
        self.order_id
    }

    /// 获取当前消息ID
    pub fn get_msg_id(&self) -> u64 {
        self.msg_id
    }

    /// 设置操作日志ID
    pub fn set_operation_log_id(&mut self, id: u64) {
        log::debug!("set operation_log id {}", id);
        self.operation_log_id = id;
    }

    /// 设置交易ID
    pub fn set_trade_id(&mut self, id: u64) {
        log::debug!("set trade id {}", id);
        self.trade_id = id;
    }

    /// 设置订单ID
    pub fn set_order_id(&mut self, id: u64) {
        log::debug!("set order id {}", id);
        self.order_id = id;
    }

    /// 设置消息ID
    pub fn set_msg_id(&mut self, id: u64) {
        log::debug!("set msg id {}", id);
        self.msg_id = id;
    }
}

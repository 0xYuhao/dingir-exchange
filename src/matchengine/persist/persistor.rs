use crate::history::HistoryWriter;
use crate::matchengine::market::{Order, Trade};
use crate::message::{self, MessageManager, OrderMessage};
pub use crate::models::{AccountDesc, BalanceHistory, InternalTx};
use crate::types::OrderEventType;

///////////////////////////// PersistExector interface ////////////////////////////

// TODO: fix methods, use ref or value?
pub trait PersistExector: Send + Sync {
    // 检查服务是否可用
    fn service_available(&self) -> bool {
        true
    }
    // make sure all data has been persisted
    //fn flush(&self) {
    //}
    fn real_persist(&self) -> bool {
        true
    }

    // 持久化余额变更记录
    fn put_balance(&mut self, balance: &BalanceHistory);
    // 持久化充值记录
    fn put_deposit(&mut self, balance: &BalanceHistory);
    // 持久化提现记录
    fn put_withdraw(&mut self, balance: &BalanceHistory);
    // 持久化内部转账记录
    fn put_transfer(&mut self, tx: InternalTx);
    // 持久化订单信息
    fn put_order(&mut self, order: &Order, at_step: OrderEventType);
    // 持久化交易记录
    fn put_trade(&mut self, trade: &Trade);
    // 注册用户信息
    fn register_user(&mut self, user: AccountDesc);
}
// 代码实现了几种不同的持久化执行器:

// 1. DummyPersistor - 空实现,不做任何持久化操作
// 2. MemBasedPersistor - 基于内存的持久化,将数据保存在内存中的messages向量里
// 3. FileBasedPersistor - 基于文件的持久化,将数据以JSON格式写入文件
// 4. MessengerBasedPersistor - 基于消息的持久化,通过MessageManager发送消息
// 5. DBBasedPersistor - 基于数据库的持久化,通过HistoryWriter写入数据库
// 6. CompositePersistor - 组合持久化器,可以同时使用多个持久化器

// 主要功能:
// 1. 提供统一的持久化接口
// 2. 支持多种持久化方式
// 3. 可以灵活组合使用不同的持久化方式
// 4. 处理订单、交易、余额等各类数据的持久化
// 5. 支持同步和异步的持久化操作
impl PersistExector for Box<dyn PersistExector + '_> {
    fn service_available(&self) -> bool {
        self.as_ref().service_available()
    }
    fn real_persist(&self) -> bool {
        self.as_ref().real_persist()
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_balance(balance)
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_deposit(balance)
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_withdraw(balance)
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        self.as_mut().put_transfer(tx)
    }
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        self.as_mut().put_order(order, at_step)
    }
    fn put_trade(&mut self, trade: &Trade) {
        self.as_mut().put_trade(trade)
    }
    fn register_user(&mut self, user: AccountDesc) {
        self.as_mut().register_user(user)
    }
}

impl PersistExector for &mut Box<dyn PersistExector + '_> {
    fn service_available(&self) -> bool {
        self.as_ref().service_available()
    }
    fn real_persist(&self) -> bool {
        self.as_ref().real_persist()
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_balance(balance)
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_deposit(balance)
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        self.as_mut().put_withdraw(balance)
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        self.as_mut().put_transfer(tx)
    }
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        self.as_mut().put_order(order, at_step)
    }
    fn put_trade(&mut self, trade: &Trade) {
        self.as_mut().put_trade(trade)
    }
    fn register_user(&mut self, user: AccountDesc) {
        self.as_mut().register_user(user)
    }
}

///////////////////////////// DummyPersistor  ////////////////////////////

// do nothing

#[derive(Default)]
pub struct DummyPersistor {}
impl DummyPersistor {
    pub fn new() -> Self {
        DummyPersistor {}
    }
    pub fn new_box() -> Box<Self> {
        Box::new(DummyPersistor {})
    }
}
impl PersistExector for DummyPersistor {
    fn real_persist(&self) -> bool {
        false
    }
    fn put_balance(&mut self, _balance: &BalanceHistory) {}
    fn put_deposit(&mut self, _balance: &BalanceHistory) {}
    fn put_withdraw(&mut self, _balance: &BalanceHistory) {}
    fn put_transfer(&mut self, _tx: InternalTx) {}
    fn put_order(&mut self, _order: &Order, _as_step: OrderEventType) {}
    fn put_trade(&mut self, _trade: &Trade) {}
    fn register_user(&mut self, _user: AccountDesc) {}
}

impl PersistExector for &mut DummyPersistor {
    fn real_persist(&self) -> bool {
        false
    }
    fn put_balance(&mut self, _balance: &BalanceHistory) {}
    fn put_deposit(&mut self, _balance: &BalanceHistory) {}
    fn put_withdraw(&mut self, _balance: &BalanceHistory) {}
    fn put_transfer(&mut self, _tx: InternalTx) {}
    fn put_order(&mut self, _order: &Order, _as_step: OrderEventType) {}
    fn put_trade(&mut self, _trade: &Trade) {}
    fn register_user(&mut self, _user: AccountDesc) {}
}

///////////////////////////// MemBasedPersistor ////////////////////////////

#[derive(Default)]
pub struct MemBasedPersistor {
    pub messages: Vec<crate::message::Message>,
}
impl MemBasedPersistor {
    pub fn new() -> Self {
        Self { messages: Vec::new() }
    }
}

impl PersistExector for MemBasedPersistor {
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        self.messages
            .push(message::Message::OrderMessage(Box::new(OrderMessage::from_order(order, at_step))));
    }
    fn put_trade(&mut self, trade: &Trade) {
        self.messages.push(message::Message::TradeMessage(Box::new(trade.clone())));
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        self.messages.push(message::Message::BalanceMessage(Box::new(balance.into())));
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        self.messages.push(message::Message::DepositMessage(Box::new(balance.into())));
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        self.messages.push(message::Message::WithdrawMessage(Box::new(balance.into())));
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        self.messages.push(message::Message::TransferMessage(Box::new(tx.into())));
    }
    fn register_user(&mut self, user: AccountDesc) {
        self.messages.push(message::Message::UserMessage(Box::new(user.into())));
    }
}

///////////////////////////// FileBasedPersistor ////////////////////////////

pub struct FileBasedPersistor {
    output_file: std::fs::File,
}
impl FileBasedPersistor {
    pub fn new(output_file_name: &str) -> Self {
        let output_file = std::fs::File::create(output_file_name).unwrap();
        Self { output_file }
    }
    pub fn write_msg(&mut self, msg: message::Message) {
        use std::io::Write;
        let s = serde_json::to_string(&msg).unwrap();
        self.output_file.write_fmt(format_args!("{}\n", s)).unwrap();
    }
}

impl PersistExector for FileBasedPersistor {
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        let msg = message::Message::OrderMessage(Box::new(OrderMessage::from_order(order, at_step)));
        self.write_msg(msg);
    }
    fn put_trade(&mut self, trade: &Trade) {
        let msg = message::Message::TradeMessage(Box::new(trade.clone()));
        self.write_msg(msg);
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        let msg = message::Message::BalanceMessage(Box::new(balance.into()));
        self.write_msg(msg);
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        let msg = message::Message::DepositMessage(Box::new(balance.into()));
        self.write_msg(msg);
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        let msg = message::Message::WithdrawMessage(Box::new(balance.into()));
        self.write_msg(msg);
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        let msg = message::Message::TransferMessage(Box::new(tx.into()));
        self.write_msg(msg);
    }
    fn register_user(&mut self, user: AccountDesc) {
        let msg = message::Message::UserMessage(Box::new(user.into()));
        self.write_msg(msg);
    }
}

///////////////////////////// MessengerBasedPersistor  ////////////////////////////

pub struct MessengerBasedPersistor {
    inner: Box<dyn MessageManager>,
}

impl MessengerBasedPersistor {
    pub fn new(inner: Box<dyn MessageManager>) -> Self {
        Self { inner }
    }
}

impl PersistExector for MessengerBasedPersistor {
    fn service_available(&self) -> bool {
        if self.inner.is_block() {
            log::warn!("message_manager full");
            return false;
        }
        true
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        self.inner.push_balance_message(&balance.into());
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        self.inner.push_deposit_message(&balance.into());
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        self.inner.push_withdraw_message(&balance.into());
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        self.inner.push_transfer_message(&tx.into());
    }
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        self.inner.push_order_message(&OrderMessage::from_order(order, at_step));
    }
    fn put_trade(&mut self, trade: &Trade) {
        self.inner.push_trade_message(trade);
    }
    fn register_user(&mut self, user: AccountDesc) {
        self.inner.push_user_message(&user.into());
    }
}

///////////////////////////// DBBasedPersistor  ////////////////////////////
///
pub struct DBBasedPersistor {
    inner: Box<dyn HistoryWriter>,
}

impl DBBasedPersistor {
    pub fn new(inner: Box<dyn HistoryWriter>) -> Self {
        Self { inner }
    }
}

impl PersistExector for DBBasedPersistor {
    fn service_available(&self) -> bool {
        if self.inner.is_block() {
            log::warn!("history_writer full");
            return false;
        }
        true
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        self.inner.append_balance_history(balance.clone());
    }
    fn put_deposit(&mut self, _balance: &BalanceHistory) {
        // TODO
    }
    fn put_withdraw(&mut self, _balance: &BalanceHistory) {
        // TODO
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        self.inner.append_internal_transfer(tx);
    }
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        //only persist on finish
        match at_step {
            OrderEventType::FINISH => self.inner.append_order_history(order),
            OrderEventType::EXPIRED => self.inner.append_expired_order_history(order),
            OrderEventType::PUT => (),
            _ => (),
        }
    }
    fn put_trade(&mut self, trade: &Trade) {
        self.inner.append_pair_user_trade(trade);
    }
    fn register_user(&mut self, user: AccountDesc) {
        self.inner.append_user(user);
    }
}

///////////////////////////// CompositePersistor  ////////////////////////////
///
#[derive(Default)]
pub struct CompositePersistor {
    persistors: Vec<Box<dyn PersistExector>>,
}

impl CompositePersistor {
    pub fn add_persistor(&mut self, p: Box<dyn PersistExector>) {
        self.persistors.push(p)
    }
}

impl PersistExector for CompositePersistor {
    fn service_available(&self) -> bool {
        for p in &self.persistors {
            if !p.service_available() {
                return false;
            }
        }
        true
    }
    fn put_balance(&mut self, balance: &BalanceHistory) {
        for p in &mut self.persistors {
            p.put_balance(balance);
        }
    }
    fn put_deposit(&mut self, balance: &BalanceHistory) {
        for p in &mut self.persistors {
            p.put_deposit(balance);
        }
    }
    fn put_withdraw(&mut self, balance: &BalanceHistory) {
        for p in &mut self.persistors {
            p.put_withdraw(balance);
        }
    }
    fn put_transfer(&mut self, tx: InternalTx) {
        for p in &mut self.persistors {
            p.put_transfer(tx.clone());
        }
    }
    fn put_order(&mut self, order: &Order, at_step: OrderEventType) {
        for p in &mut self.persistors {
            p.put_order(order, at_step);
        }
    }
    fn put_trade(&mut self, trade: &Trade) {
        for p in &mut self.persistors {
            p.put_trade(trade);
        }
    }
    fn register_user(&mut self, user: AccountDesc) {
        for p in &mut self.persistors {
            p.register_user(user.clone());
        }
    }
}

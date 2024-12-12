use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;
// 使用lazy_static创建一个全局字符串池
// 这是一个线程安全的HashMap，用于存储已经内部化的字符串
lazy_static! {
    pub static ref STRING_POOL: Mutex<HashMap<String, &'static str>> = Default::default();
}

// don't make this function From<XXX>. We'd better call this explicitly
// prevent any unintentional mem leak 避免无意中的内存泄漏
// 字符串内部化函数
// 将输入的字符串存储到全局字符串池中，返回静态生命周期的字符串引用
// 这样可以避免重复的字符串分配，节省内存
pub fn intern_string(s: &str) -> &'static str {
    *STRING_POOL
        .lock()
        .unwrap()
        .entry(s.to_owned())
        .or_insert_with(|| Box::leak(s.to_string().into_boxed_str()))
}

// InternedString结构体，包装了一个静态生命周期的字符串引用
// 实现了Debug、Clone、Copy和Default特征
#[derive(Debug, Clone, Copy, Default)]
pub struct InternedString(&'static str);

// 从静态字符串引用转换为InternedString
impl From<&'static str> for InternedString {
    fn from(str: &'static str) -> Self {
        InternedString(str)
    }
}
// 从InternedString转换回静态字符串引用
impl From<InternedString> for &'static str {
    fn from(str: InternedString) -> Self {
        str.0
    }
}
// 实现Deref特征，允许InternedString直接调用str的方法
impl std::ops::Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}
// 实现序列化特征，使InternedString可以被序列化
impl serde::ser::Serialize for InternedString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}
// 实现反序列化特征，使InternedString可以被反序列化
// 在反序列化时会自动进行字符串内部化
impl<'de> serde::de::Deserialize<'de> for InternedString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(intern_string(&s).into())
    }
}

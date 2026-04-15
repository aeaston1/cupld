use super::Value;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PropertyMap {
    entries: Vec<(String, Value)>,
}

impl PropertyMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_pairs<I, K>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut map = Self::new();
        for (key, value) in pairs {
            map.insert(key, value);
        }
        map
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(existing, _)| existing == key)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.entries
            .iter_mut()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    }

    pub fn insert<K>(&mut self, key: K, value: Value) -> Option<Value>
    where
        K: Into<String>,
    {
        let key = key.into();
        if let Some((_, existing)) = self.entries.iter_mut().find(|(current, _)| current == &key) {
            return Some(std::mem::replace(existing, value));
        }

        self.entries.push((key, value));
        None
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let idx = self
            .entries
            .iter()
            .position(|(existing, _)| existing == key)?;
        Some(self.entries.remove(idx).1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(key, _)| key.as_str())
    }
}

impl IntoIterator for PropertyMap {
    type Item = (String, Value);
    type IntoIter = std::vec::IntoIter<(String, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl From<PropertyMap> for Value {
    fn from(value: PropertyMap) -> Self {
        Self::Map(value.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{PropertyMap, Value};

    #[test]
    fn preserves_insertion_order_for_keys() {
        let mut properties = PropertyMap::new();
        properties.insert("name", Value::from("Ada"));
        properties.insert("role", Value::from("engineer"));
        properties.insert("name", Value::from("Grace"));

        let keys = properties.keys().collect::<Vec<_>>();
        assert_eq!(keys, vec!["name", "role"]);
        assert_eq!(properties.get("name"), Some(&Value::from("Grace")));
    }
}

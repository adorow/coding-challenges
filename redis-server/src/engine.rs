use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::ops::Add;
use std::time::Duration;

#[cfg(test)]
use mock_instant::global::SystemTime;

#[cfg(not(test))]
use std::time::SystemTime;
use crate::engine::Value::StringValue;

pub struct StorageEngine {
    // todo: this works fine to start with get/set, need to review for other types perhaps
    map: HashMap<String, Item>,
}

pub enum TimeToLive {
    KeyDoesNotExist,
    DoesNotExpire,
    ExpiresInSeconds(u64),
}

struct Item {
    value: Value,
    expires_at: Option<SystemTime>,
}

// todo: to try and support operations on other data types
enum Value {
    StringValue(String),
}

impl Value {
    fn get_string(&self) -> Result<&String, String> {
        match self {
            StringValue(value) => Ok(value),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }
}

impl StorageEngine {
    pub fn new() -> StorageEngine {
        StorageEngine {
            map: HashMap::new(),
        }
    }

    /// Generic (and private) 'get_item' that contains necessary retrieval logic and is used by multiple functions.
    ///
    /// This function handles:
    /// - item expiry
    fn get_item(&mut self, key: &str) -> Option<&Item> {
        let now = SystemTime::now();
        match self.map.entry(String::from(key)) {
            Occupied(entry) => {
                if let Some(expires_at) = entry.get().expires_at {
                    if expires_at < now {
                        entry.remove();
                        return None
                    }
                }

                // 'entry.get()' returns a reference with lifetime of "entry"
                // 'entry.into_mut()' is the only one that returns a reference with lifetime of the HashMap
                // - so that's what's used - even though a mutable reference is not needed
                Some(entry.into_mut())
            }
            Vacant(_) => None,
        }
    }

    // 'get' requires a mutable reference because of how the expiry mechanism is implemented
    pub fn get(&mut self, key: &str) -> Result<Option<&String>, String> {
        self.get_item(key)
            .map(|item|item.value.get_string())
            .transpose()
    }

    pub fn set(&mut self, key: String, value: String, expiry_seconds: Option<u64>) -> Result<(), String> {
        // calculate expiry, if any
        let expires_at =
            expiry_seconds.map(|exp| SystemTime::now().add(Duration::from_secs(exp)));

        self.map.insert(key, Item { value: StringValue(value), expires_at });

        // always succeeds because it overwrites existing values
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let removed = self.map.remove(key);
        removed.is_some()
    }

    pub fn exists(&mut self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn time_to_live(&mut self, key: &str) -> TimeToLive {
        match self.get_item(key) {
            None => TimeToLive::KeyDoesNotExist,
            Some(item) => {
                match item.expires_at {
                    None => TimeToLive::DoesNotExpire,
                    Some(expires_at) => {
                        SystemTime::now().duration_since(expires_at)
                            .map(|duration| TimeToLive::ExpiresInSeconds(duration.as_secs()))
                            // don't expect 'duration_since' to ever Err here, so falling back to does not expire if this ever happens
                            .unwrap_or_else(|err| {
                                eprintln!("Error calculating expiry duration for {}: {}. Falling back to 'DoesNotExpire'", key, err);
                                TimeToLive::DoesNotExpire
                            })
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use mock_instant::global::MockClock;

    #[test]
    fn get_should_return_nil_when_unset() {
        let mut engine = StorageEngine::new();

        let result = engine.get("foo").unwrap();
        assert_eq!(result, None)
    }

    #[test]
    fn get_should_return_the_previously_set_value_when_no_ttl_is_defined() {
        let mut engine = StorageEngine::new();

        engine.set(String::from("foo"), String::from("bar"), None).unwrap();

        let result = engine.get("foo").unwrap();
        assert_eq!(result, Some(&"bar".to_owned()));
    }

    #[test]
    fn get_should_return_the_previously_set_value_or_not_based_on_whether_ttl_is_expired() {
        let mut engine = StorageEngine::new();

        // set the value with ttl=10s
        engine.set(String::from("foo"), String::from("bar"), Some(10)).unwrap();

        // fetch the value after 1s
        MockClock::advance_system_time(Duration::from_secs(1));
        let result = engine.get("foo").unwrap();
        assert_eq!(result, Some(&"bar".to_owned()));

        // fetch the value after 7s more (8s total)
        MockClock::advance_system_time(Duration::from_secs(7));
        let result = engine.get("foo").unwrap();
        assert_eq!(result, Some(&"bar".to_owned()));

        // fetch the value after another 3s more (11s total) -> TTL expired
        MockClock::advance_system_time(Duration::from_secs(7));
        let result = engine.get("foo").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn remove_should_remove_and_indicate_if_something_was_removed_or_not() {
        let mut engine = StorageEngine::new();

        engine.set(String::from("foo"), String::from("bar"), None).unwrap();

        let result = engine.remove("foo");
        assert_eq!(result, true);

        // since the value was removed, it can no longer be found
        let result = engine.get("foo").unwrap();
        assert_eq!(result, None);

        // a new 'remove' will now not remove anything
        let result = engine.remove("foo");
        assert_eq!(result, false);
    }


    #[test]
    fn exists_should_tell_whether_an_entry_exists_for_key() {
        let mut engine = StorageEngine::new();

        let key = String::from("foo");

        // initially doesn't exist
        assert_eq!(engine.exists(&key), false);

        // after setting, exists
        engine.set(key.clone(), String::from("bar"), None).unwrap();

        assert_eq!(engine.exists(&key), true);
    }
}

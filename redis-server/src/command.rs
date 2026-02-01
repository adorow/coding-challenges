use crate::engine::{StorageEngine, TimeToLive};
use crate::protocol::RespObject;
use crate::protocol::RespObject::{Array, BulkString, Error, Integer, NullBulkString, SimpleString};

// public struct to explicitly hide implementation details from enum RespCommand and its children
// enums can only have public components, and I want some of those details to be hidden
#[derive(Debug, Eq, PartialEq)]
pub struct Command(RespCommand);

impl Command {
    pub fn from(input: RespObject) -> Result<Command, String> {
        RespCommand::from(input)
            .map(|inner| Self(inner))
    }

    // TODO: can create some specific functions to create the different commands, eg: ping(), echo(String), etc ...

    pub fn execute_on(&self, engine: &mut StorageEngine) -> RespObject {
        self.0.execute_on(engine)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct GetCommand {
    key: String,
}

impl GetCommand {
    pub fn from(key: String) -> GetCommand {
        GetCommand { key }
    }

    fn execute_on<'a>(&self, engine: &'a mut StorageEngine) -> Result<Option<&'a String>, String> {
        engine.get(&self.key)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SetCommand {
    key: String,
    value: String,
    expiry_seconds: Option<u64>,
}

impl SetCommand {
    pub fn from_key_value(key_value: (String, String)) -> SetCommand {
        SetCommand { key: key_value.0, value: key_value.1, expiry_seconds: None }
    }

    pub fn from(key_value: (String, String), expiry_seconds: Option<u64>) -> SetCommand {
        SetCommand { key: key_value.0, value: key_value.1, expiry_seconds }
    }

    fn execute_on(&self, engine: &mut StorageEngine) -> Result<(), String> {
        // todo: find something more efficient, so .clone() doesn't have to be called here
        engine.set(self.key.clone(), self.value.clone(), self.expiry_seconds.clone())?;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct MsetCommand {
    commands: Vec<SetCommand>,
}

impl MsetCommand {
    pub fn from_key_values(key_values: Vec<(String, String)>) -> MsetCommand {
        let commands = key_values.into_iter()
            .map(|kv| SetCommand::from_key_value(kv))
            .collect();
        MsetCommand { commands }
    }

    fn execute_on(&self, engine: &mut StorageEngine) -> Result<(), String> {
        self.commands.iter()
            .for_each(|cmd| cmd.execute_on(engine).unwrap());
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct MgetCommand {
    commands: Vec<GetCommand>,
}

impl MgetCommand {
    pub fn from_keys(keys: Vec<String>) -> MgetCommand {
        MgetCommand {
            commands: keys.into_iter().map(|k| GetCommand::from(k)).collect()
        }
    }

    fn execute_on(&self, engine: &mut StorageEngine) -> Vec<Option<String>> {
        self.commands.iter()
            // todo: maybe there's a better solution, but for now _must_ clone and
            //  return Option<String> instead of Option<&String>;
            //  problem is that calling in loop, technically the reference returned e.g. in the first loop
            //  will not exist anymore after the second loop (because that second call may deallocate it)
            //  a solution may be to implement the multi_get into Engine at a low level
            .map(|cmd| match cmd.execute_on(engine) {
                Ok(val) => val.cloned(),
                // on MGET 'nil' is returned in case of wrong type
                Err(_) => None
            })
            .collect()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DelCommand {
    keys: Vec<String>,
}

impl DelCommand {
    pub fn from_keys(keys: Vec<String>) -> DelCommand {
        DelCommand { keys }
    }

    fn execute_on(&self, engine: &mut StorageEngine) -> usize {
        self.keys.iter()
            .map(|key| engine.remove(key))
            .filter(|it| *it)
            .count()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ExistsCommand {
    keys: Vec<String>,
}

impl ExistsCommand {
    pub fn from_keys(keys: Vec<String>) -> ExistsCommand {
        ExistsCommand { keys }
    }

    fn execute_on(&self, engine: &mut StorageEngine) -> usize {
        self.keys.iter()
            .map(|key| engine.exists(key))
            .filter(|it| *it)
            .count()
    }
}

#[derive(Debug, Eq, PartialEq)]
enum RespCommand {
    Ping,
    // TODO: review: do these commands really need to own this data (particularly the Strings)?
    Echo { message: String },
    Set(SetCommand),
    Get(GetCommand),
    Ttl { key: String },
    Mset(MsetCommand),
    Mget(MgetCommand),
    Del(DelCommand),
    Exists(ExistsCommand),
}

impl RespCommand {

    pub fn from(input: RespObject) -> Result<RespCommand, String> {
        match input {
            Array(entries) => {
                // TODO: the whole thing here can probably be more efficient and clean
                if entries.is_empty() {
                    return Err("Wrong number of arguments for command".to_string());
                }
                let entries = entries.iter()
                    .map(|e| if let BulkString(str) = e {
                        Ok(str.to_owned())
                    } else {
                        Err(String::from("Array should only contain BulkStrings"))
                    }).collect::<Result<Vec<String>, String>>()?;

                let mut arguments = entries.into_iter();

                let cmd_name =
                    arguments.next()
                        .map(|str| str.to_lowercase())
                        .ok_or_else(|| "Wrong number of arguments for command".to_string())?;

                match cmd_name.as_str() {
                    "ping" => Ok(RespCommand::Ping),
                    "echo" => {
                        let msg = arguments.next()
                            .ok_or_else(|| "Not enough arguments for 'echo'".to_owned())?;

                        // check too many arguments
                        if arguments.next().is_some() {
                            return Err("Wrong number of arguments for 'echo' command".to_string());
                        }

                        Ok(RespCommand::Echo { message: msg.to_owned() })
                    },
                    "get" => {
                        let key = arguments.next()
                            .ok_or_else(|| "Not enough arguments for 'get'".to_owned())?;

                        // check too many arguments
                        if arguments.next().is_some() {
                            return Err("Wrong number of arguments for 'get' command".to_string());
                        }

                        Ok(RespCommand::Get(GetCommand::from(key.to_owned())))
                    }
                    "set" => {
                        let key = arguments.next()
                            .ok_or_else(|| "Wrong number of arguments for command".to_owned())?;

                        let value = arguments.next()
                            .ok_or_else(|| "Wrong number of arguments for command".to_owned())?;

                        let mut expiry_seconds = None;

                        // the next arguments have no specific order

                        while let Some(param) = arguments.next() {
                            match param.to_lowercase().as_str() {
                                // set expiry in seconds
                                "ex" => {
                                    expiry_seconds = {
                                        let ex_value =
                                            arguments.next()
                                            .ok_or_else(|| "Wrong number of arguments for command".to_owned())?
                                            .parse::<u64>()
                                            .or_else(|_| Err("value is not an integer or out of range".to_owned()))?;

                                        Some(ex_value)
                                    }
                                }
                                _ => return Err("Wrong number of arguments for command".to_owned())
                            }
                        }

                        Ok(RespCommand::Set(SetCommand::from((key.to_owned(), value.to_owned()), expiry_seconds)))
                    }
                    "ttl" => {
                        let key = arguments.next()
                            .ok_or_else(|| "Not enough arguments for 'ttl'".to_owned())?;

                        // check too many arguments
                        if arguments.next().is_some() {
                            return Err("Wrong number of arguments for 'ttl' command".to_string());
                        }

                        Ok(RespCommand::Ttl { key: key.to_owned() })
                    }
                    "mset" => {
                        let mut key_values: Vec<(String, String)> = vec![];

                        while let Some(key) = arguments.next() {
                            let value = arguments.next()
                                .ok_or_else(|| "Not enough arguments for 'mset'".to_owned())?;

                            key_values.push((key.to_owned(), value.to_owned()));
                        }

                        if key_values.is_empty() {
                            return Err("Wrong number of arguments for 'mset' command".to_string());
                        }

                        Ok(RespCommand::Mset(MsetCommand::from_key_values(key_values)))
                    }
                    "mget" => {
                        let mut keys: Vec<String> = vec![];

                        while let Some(key) = arguments.next() {
                            keys.push(key.to_owned());
                        }

                        if keys.is_empty() {
                            return Err("Wrong number of arguments for 'mget' command".to_string());
                        }

                        Ok(RespCommand::Mget(MgetCommand::from_keys(keys)))
                    }
                    "del" => {
                        let mut keys: Vec<String> = vec![];

                        while let Some(key) = arguments.next() {
                            keys.push(key.to_owned());
                        }

                        if keys.is_empty() {
                            return Err("Wrong number of arguments for 'del' command".to_string());
                        }

                        Ok(RespCommand::Del(DelCommand::from_keys(keys)))
                    }
                    "exists" => {
                        let mut keys: Vec<String> = vec![];

                        while let Some(key) = arguments.next() {
                            keys.push(key.to_owned());
                        }

                        if keys.is_empty() {
                            return Err("Wrong number of arguments for 'exists' command".to_string());
                        }

                        Ok(RespCommand::Exists(ExistsCommand::from_keys(keys)))
                    }
                    _ => Err(format!("unknown command '{cmd_name}'")),
                }
            },
            _ => Err("An Array of BulkStrings is expected".to_string()),
        }
    }

    pub fn execute_on(&self, engine: &mut StorageEngine) -> RespObject {
        match self {
            RespCommand::Ping => SimpleString("PONG".to_string()),
            RespCommand::Echo { message} => SimpleString(message.clone()),
            RespCommand::Get(cmd) => {
                match cmd.execute_on(engine) {
                    Ok(Some(value)) => BulkString(value.clone()),
                    Ok(None) => NullBulkString,
                    Err(e) => Error(e.to_string()),
                }
            },
            RespCommand::Set(cmd) => {
                match cmd.execute_on(engine) {
                    Ok(_) => SimpleString("OK".to_string()),
                    Err(e) => Error(e.to_string()),
                }
            },
            RespCommand::Ttl { key } => {
                match engine.time_to_live(key) {
                    TimeToLive::KeyDoesNotExist => Integer(-2),
                    TimeToLive::DoesNotExpire => Integer(-1),
                    TimeToLive::ExpiresInSeconds(seconds) => Integer(seconds as i64)
                }
            },
            RespCommand::Mset(cmd) => {
                match cmd.execute_on(engine) {
                    Ok(_) => SimpleString("OK".to_string()),
                    Err(e) => Error(e.to_string()),
                }
            },
            RespCommand::Mget(cmd) => {
                let mget_results =
                    cmd.execute_on(engine)
                        .iter()
                        .map(|option| {
                            match option {
                                Some(value) => BulkString(value.clone()),
                                None => NullBulkString,
                            }
                        })
                        .collect();

                Array(mget_results)
            },
            RespCommand::Del(cmd) => {
                let deleted_count = cmd.execute_on(engine);
                Integer(deleted_count as i64)
            }
            RespCommand::Exists(cmd) => {
                let exists_count = cmd.execute_on(engine);
                Integer(exists_count as i64)
            }
        }
    }
}

#[cfg(test)]
mod command_creation_tests {
    use super::*;
    use crate::protocol::RespObject::{Error, Integer, NullArray, NullBulkString, SimpleString};

    #[test]
    fn create_ping_command() {
        let cmd = Command::from(Array(vec![BulkString("ping".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Ping)));
    }

    #[test]
    fn create_ping_command_from_uppercase() {
        let cmd = Command::from(Array(vec![BulkString("PING".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Ping)));
    }

    #[test]
    fn create_ping_command_from_mixed_case() {
        let cmd = Command::from(Array(vec![BulkString("PinG".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Ping)));
    }

    #[test]
    fn create_echo_command() {
        let cmd = Command::from(Array(vec![BulkString("echo".to_owned()), BulkString("\"Hello, world!\"".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Echo { message: String::from("\"Hello, world!\"") })));
    }

    #[test]
    fn create_plain_set_command() {
        let cmd = Command::from(Array(vec![BulkString("set".to_owned()), BulkString("Name".to_owned()), BulkString("Doe".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Set(SetCommand::from_key_value((String::from("Name"), String::from("Doe")))))));
    }

    #[test]
    fn create_set_command_with_expiry() {
        let cmd = Command::from(Array(vec![BulkString("set".to_owned()), BulkString("Name".to_owned()), BulkString("Doe".to_owned()), BulkString("EX".to_owned()), BulkString("3600".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Set( SetCommand::from((String::from("Name"), String::from("Doe")), Some(3600))))));
    }

    #[test]
    fn create_get_command() {
        let cmd = Command::from(Array(vec![BulkString("get".to_owned()), BulkString("Name".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Get(GetCommand::from(String::from("Name"))))));
    }

    #[test]
    fn create_ttl_command() {
        let cmd = Command::from(Array(vec![BulkString("ttl".to_owned()), BulkString("Name".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Ttl { key: String::from("Name") })));
    }

    #[test]
    fn cannot_create_empty_mset_command() {
        let cmd = Command::from(Array(vec![BulkString("mset".to_owned())]));
        assert_eq!(cmd, Err("Wrong number of arguments for 'mset' command".to_string()));
    }

    #[test]
    fn create_mset_command() {
        let cmd = Command::from(Array(vec![BulkString("mset".to_owned()), BulkString("FirstName".to_owned()), BulkString("Jane".to_owned()), BulkString("LastName".to_owned()), BulkString("Doe".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Mset(MsetCommand::from_key_values(vec![("FirstName".to_string(), "Jane".to_string()), ("LastName".to_string(), "Doe".to_string())])))));
    }

    #[test]
    fn cannot_create_empty_mget_command() {
        let cmd = Command::from(Array(vec![BulkString("mget".to_owned())]));
        assert_eq!(cmd, Err("Wrong number of arguments for 'mget' command".to_string()));
    }

    #[test]
    fn create_mget_command() {
        let cmd = Command::from(Array(vec![BulkString("mget".to_owned()), BulkString("FirstName".to_owned()), BulkString("LastName".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Mget(MgetCommand::from_keys(vec!["FirstName".to_string(), "LastName".to_string()])))));
    }

    #[test]
    fn cannot_create_empty_del_command() {
        let cmd = Command::from(Array(vec![BulkString("del".to_owned())]));
        assert_eq!(cmd, Err("Wrong number of arguments for 'del' command".to_string()));
    }

    #[test]
    fn create_del_command() {
        let cmd = Command::from(Array(vec![BulkString("del".to_owned()), BulkString("FirstName".to_owned()), BulkString("LastName".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Del(DelCommand::from_keys(vec!["FirstName".to_string(), "LastName".to_string()])))));
    }

    #[test]
    fn create_exists_command() {
        let cmd = Command::from(Array(vec![BulkString("exists".to_owned()), BulkString("FirstName".to_owned()), BulkString("LastName".to_owned())]));
        assert_eq!(cmd, Ok(Command(RespCommand::Exists(ExistsCommand::from_keys(vec!["FirstName".to_string(), "LastName".to_string()])))));
    }

    #[test]
    fn cannot_create_non_existing_command() {
        let cmd = Command::from(Array(vec![BulkString("whubalubadubdub".to_owned())]));
        assert_eq!(cmd, Err("unknown command 'whubalubadubdub'".to_owned()));
    }

    #[test]
    fn cannot_create_command_from_empty_array() {
        let cmd = Command::from(Array(vec![]));
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_null_array() {
        let cmd = Command::from(NullArray);
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_array_that_doesnt_contain_only_bulk_strings() {
        let cmd = Command::from(Array(vec![Integer(4)]));
        assert_eq!(cmd, Err("Array should only contain BulkStrings".to_owned()));
    }

    #[test]
    fn cannot_create_command_from_error() {
        let cmd = Command::from(Error(String::from("Error message")));
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_integer() {
        let cmd = Command::from(Integer(0));
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_simple_string() {
        let cmd = Command::from(SimpleString("foobar".to_owned()));
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_bulk_string() {
        // 'PING' is a valid command, but commands are expected to come in an Array
        let cmd = Command::from(BulkString("PING".to_owned()));
        assert!(cmd.is_err());
    }

    #[test]
    fn cannot_create_command_from_null_bulk_string() {
        let cmd = Command::from(NullBulkString);
        assert!(cmd.is_err());
    }
}

#[cfg(test)]
mod command_execution_tests {
    use crate::command::{Command, DelCommand, ExistsCommand, GetCommand, MgetCommand, MsetCommand, RespCommand, SetCommand};
    use crate::engine::StorageEngine;
    use crate::protocol::RespObject::{Array, BulkString, Integer, NullBulkString, SimpleString};

    #[test]
    fn execute_ping_should_return_pong() {
        let mut engine = StorageEngine::new();
        let cmd = Command(RespCommand::Ping);

        let result = cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString("PONG".to_owned()));
    }

    #[test]
    fn execute_echo_should_return_first_parameter() {
        let mut engine = StorageEngine::new();
        let cmd = Command(RespCommand::Echo { message: String::from("\"Hello, world\"") });

        let result = cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString(String::from("\"Hello, world\"")));
    }

    #[test]
    fn execute_get_should_return_nil_when_unset() {
        let mut engine = StorageEngine::new();
        let cmd = Command(RespCommand::Get(GetCommand::from(String::from("foo"))));

        let result = cmd.execute_on(&mut engine);
        assert_eq!(result, NullBulkString);
    }

    #[test]
    fn execute_get_should_return_the_previously_set_value() {
        let mut engine = StorageEngine::new();
        let set_cmd = Command(RespCommand::Set( SetCommand::from_key_value((String::from("foo"), String::from("bar")))));
        let get_cmd = Command(RespCommand::Get(GetCommand::from(String::from("foo"))));

        let result = set_cmd.execute_on(&mut engine);
        // SET responds with a simple string of 'OK'
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = get_cmd.execute_on(&mut engine);
        assert_eq!(result, BulkString("bar".to_owned()));
    }

    #[test]
    fn execute_get_should_return_the_previously_mset_values() {
        let mut engine = StorageEngine::new();
        let mset_cmd = Command(RespCommand::Mset(MsetCommand::from_key_values(vec![(String::from("key1"), String::from("1")), (String::from("key2"), String::from("2"))])));
        let get_cmd1 = Command(RespCommand::Get(GetCommand::from(String::from("key1"))));
        let get_cmd2 = Command(RespCommand::Get(GetCommand::from(String::from("key2"))));

        let result = mset_cmd.execute_on(&mut engine);
        // MSET responds with a simple string of 'OK'
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = get_cmd1.execute_on(&mut engine);
        assert_eq!(result, BulkString("1".to_owned()));

        let result = get_cmd2.execute_on(&mut engine);
        assert_eq!(result, BulkString("2".to_owned()));
    }

    #[test]
    fn execute_mset_with_repeated_key_applies_the_last_value() {
        let mut engine = StorageEngine::new();
        let mset_cmd = Command(RespCommand::Mset(MsetCommand::from_key_values(vec![(String::from("foo"), String::from("bar")), (String::from("foo"), String::from("baz"))])));
        let get_cmd = Command(RespCommand::Get(GetCommand::from(String::from("foo"))));

        let result = mset_cmd.execute_on(&mut engine);
        // MSET responds with a simple string of 'OK'
        assert_eq!(result, SimpleString("OK".to_owned()));

        // will return the second value, because it overwrites the first entry in mset
        let result = get_cmd.execute_on(&mut engine);
        assert_eq!(result, BulkString("baz".to_owned()));
    }

    #[test]
    fn execute_mget_should_return_all_previously_set_or_mset_values() {
        let mut engine = StorageEngine::new();
        let set_cmd = Command(RespCommand::Set(SetCommand::from_key_value((String::from("fromSet"), String::from("set")))));
        let mset_cmd = Command(RespCommand::Mset(MsetCommand::from_key_values(vec![(String::from("fromMset"), String::from("mset"))])));
        let mget_cmd = Command(RespCommand::Mget(MgetCommand::from_keys(vec![String::from("fromSet"), String::from("fromMset"), String::from("fromNonExistent")])));

        let result = set_cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = mset_cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = mget_cmd.execute_on(&mut engine);
        // the set values are returning in the order requested, non-existing values are Null(BulkString)
        assert_eq!(result, Array(vec![BulkString(String::from("set")), BulkString(String::from("mset")), NullBulkString]));
    }

    #[test]
    fn execute_del_removes_previously_set_values() {
        let mut engine = StorageEngine::new();
        let mset_cmd = Command(RespCommand::Mset(MsetCommand::from_key_values(vec![(String::from("key1"), String::from("value1")), (String::from("key2"), String::from("value2"))])));
        let del_cmd = Command(RespCommand::Del(DelCommand::from_keys(vec![String::from("key1"), String::from("key2"), String::from("key3")])));
        let mget_cmd = Command(RespCommand::Mget(MgetCommand::from_keys(vec![String::from("key1"), String::from("key2")])));

        let result = mset_cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = del_cmd.execute_on(&mut engine);
        // 'del' response tells us how many items were removed
        assert_eq!(result, Integer(2));

        let result = mget_cmd.execute_on(&mut engine);
        // getting the deleted keys shows that they were deleted
        assert_eq!(result, Array(vec![NullBulkString, NullBulkString]));
    }

    #[test]
    fn execute_exists_returns_the_count_of_existing_keys() {
        let mut engine = StorageEngine::new();
        let mset_cmd = Command(RespCommand::Mset(MsetCommand::from_key_values(vec![(String::from("key1"), String::from("value1")), (String::from("key2"), String::from("value2"))])));
        let exists_cmd = Command(RespCommand::Exists(ExistsCommand::from_keys(vec![String::from("key1"), String::from("key2"), String::from("key3")])));
        let mget_cmd = Command(RespCommand::Mget(MgetCommand::from_keys(vec![String::from("key1"), String::from("key2")])));

        let result = mset_cmd.execute_on(&mut engine);
        assert_eq!(result, SimpleString("OK".to_owned()));

        let result = exists_cmd.execute_on(&mut engine);
        // 'exists' response tells us how many items exist
        assert_eq!(result, Integer(2));
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::deny_list_v1::{
    get_coin_deny_list, get_deny_list_root_object, DENY_LIST_COIN_TYPE_INDEX, DENY_LIST_MODULE,
};
use crate::dynamic_field::get_dynamic_field_from_store;
use crate::id::UID;
use crate::storage::ObjectStore;
use crate::{MoveTypeTagTrait, SUI_FRAMEWORK_PACKAGE_ID};
use move_core_types::ident_str;
use move_core_types::language_storage::{StructTag, TypeTag};
use serde::{Deserialize, Serialize};

/// Rust representation of the Move type 0x2::config::Config.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    id: UID,
}

/// Rust representation of the Move type 0x2::config::Setting.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Setting<V: Copy> {
    id: UID,
    data: Option<SettingData<V>>,
}

/// Rust representation of the Move type 0x2::config::SettingData.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct SettingData<V: Copy> {
    newer_value_epoch: u64,
    newer_value: V,
    older_value_opt: Option<V>,
}

/// Rust representation of the Move type 0x2::coin::DenyCapV2.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DenyCapV2 {
    pub id: UID,
    pub allow_global_pause: bool,
}

/// Rust representation of the Move type 0x2::deny_list::ConfigKey.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ConfigKey {
    per_type_index: u64,
    per_type_key: Vec<u8>,
}

impl MoveTypeTagTrait for ConfigKey {
    fn get_type_tag() -> TypeTag {
        TypeTag::Struct(Box::new(StructTag {
            address: SUI_FRAMEWORK_PACKAGE_ID.into(),
            module: DENY_LIST_MODULE.to_owned(),
            name: ident_str!("ConfigKey").to_owned(),
            type_params: vec![],
        }))
    }
}

pub fn get_per_type_coin_deny_list_v2(coin_type: String, object_store: &dyn ObjectStore) {
    let deny_list_root =
        get_deny_list_root_object(object_store).expect("Deny list root object not found");
    get_dynamic_field_from_store(
        object_store,
        deny_list_root.id(),
        &ConfigKey {
            per_type_index: DENY_LIST_COIN_TYPE_INDEX,
            per_type_key: coin_type.as_bytes().to_vec(),
        },
    );
}

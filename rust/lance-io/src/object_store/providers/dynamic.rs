// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::env;
use std::ffi::{CStr, CString};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use super::ObjectStore;
use crate::object_store::{ObjectStoreParams, ObjectStoreProvider};
use lance_core::error::{Error, Result};
use object_store::path::Path;
use snafu::location;
use url::Url;

/// The environment variable which associates dynamically loaded object store providers with prefixes.
/// For example 'foo:/my/library/path.so'
const LANCE_DYNAMICALLY_LOADED_OBJECT_STORE_PROVIDERS: &str =
    "LANCE_DYNAMICALLY_LOADED_OBJECT_STORE_PROVIDERS";

/// The C function name which we use to load the new object store provider from the dynamically loaded library.
const CREATE_NEW_OBJECT_STORE_PROVIDER: &str = "create_new_object_store_provider";

pub fn dynamic_providers_from_env() -> Result<Vec<DynamicBlobStoreProvider>> {
    dynamic_providers_from_string(
        LANCE_DYNAMICALLY_LOADED_OBJECT_STORE_PROVIDERS,
        &env::var(LANCE_DYNAMICALLY_LOADED_OBJECT_STORE_PROVIDERS)
            .ok()
            .unwrap_or("".to_string()),
    )
}

pub fn dynamic_providers_from_string(what: &str, input: &str) -> Result<Vec<DynamicBlobStoreProvider>> {
    let mut providers = Vec::<DynamicBlobStoreProvider>::new();
    for provider_string in input.split(",") {
        if !provider_string.is_empty() {
            let provider = dynamic_provider_from_string(what, provider_string)?;
            providers.push(provider);
        }
    }
    Ok(providers)
}

pub fn dynamic_provider_from_string(what: &str, input: &str) -> Result<DynamicBlobStoreProvider> {
    let index = match input.find(":") {
        None => {
            return Err(Error::invalid_input(
                format!("No colon found in {}.", what),
                location!(),
            ))
        }
        Some(index) => index,
    };
    DynamicBlobStoreProvider::new(&input[..index], &input[index + 1..])
}

fn cstr(input: &str) -> Result<CString> {
    match CString::new(input) {
        Ok(cstr) => Ok(cstr),
        Err(err) => Err(Error::invalid_input(
            format!("CString::new({}) failed: {}", input, err),
            location!(),
        )),
    }
}

fn dlerror() -> String {
    let c_ptr = unsafe { libc::dlerror() };
    if c_ptr.is_null() {
        "unknown dlerror".to_string()
    } else {
        unsafe { CStr::from_ptr(c_ptr) }
            .to_string_lossy()
            .to_string()
    }
}

#[derive(Debug)]
struct ProviderLib {
    lib: *mut libc::c_void,
    provider: Box<dyn ObjectStoreProvider>,
}

impl Drop for ProviderLib {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.lib);
        };
    }
}

unsafe impl Send for ProviderLib {}
unsafe impl Sync for ProviderLib {}

struct ProviderLibs {
    libs: HashMap<String, Weak<ProviderLib>>,
}

impl ProviderLibs {
    fn load(&mut self, path: &str) -> Result<Arc<ProviderLib>> {
        if let Some(entry) = self.libs.get(path) {
            if let Some(arc) = entry.upgrade() {
                return Ok(arc);
            }
        }
        let path_str = cstr(path)?;
        let create_new_object_store_provider_str = cstr(CREATE_NEW_OBJECT_STORE_PROVIDER)?;
        let lib =
            unsafe { libc::dlopen(path_str.as_ptr() as *const libc::c_char, libc::RTLD_LAZY) };
        if lib.is_null() {
            return Err(Error::invalid_input(
                format!("dlopen({}) failed: {}", path, dlerror()),
                location!(),
            ));
        }
        let symbol = unsafe {
            libc::dlsym(
                lib,
                create_new_object_store_provider_str.as_ptr() as *const libc::c_char,
            )
        };
        if symbol.is_null() {
            unsafe {
                libc::dlclose(lib);
            }
            return Err(Error::invalid_input(
                format!(
                    "dlsym({}) failed: {}",
                    CREATE_NEW_OBJECT_STORE_PROVIDER,
                    dlerror()
                ),
                location!(),
            ));
        }
        let create_new_object_store_provider: fn() -> Box<dyn ObjectStoreProvider> =
            unsafe { std::mem::transmute(symbol) };
        let provider = (create_new_object_store_provider)();
        let provider_lib = ProviderLib { lib, provider };
        let arc = Arc::new(provider_lib);
        self.libs.insert(path.to_string(), Arc::downgrade(&arc));
        Ok(arc)
    }
}

static PROVIDER_LIBS: LazyLock<Mutex<ProviderLibs>> = LazyLock::new(|| {
    Mutex::new(ProviderLibs {
        libs: HashMap::<String, Weak<ProviderLib>>::new(),
    })
});

#[derive(Debug)]
pub struct DynamicBlobStoreProvider {
    scheme: String,
    lib: Arc<ProviderLib>,
}

impl DynamicBlobStoreProvider {
    pub fn new(scheme: &str, path: &str) -> Result<DynamicBlobStoreProvider> {
        let lib = PROVIDER_LIBS.lock().unwrap().load(path)?;
        Ok(Self {
            scheme: scheme.to_string(),
            lib: lib,
        })
    }

    pub fn scheme(&self) -> String {
        self.scheme.clone()
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for DynamicBlobStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        self.lib.provider.new_store(base_path, params).await
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        self.lib.provider.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        scheme: &str,
        authority: &str,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        self.lib
            .provider
            .calculate_object_store_prefix(scheme, authority, storage_options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_core::utils::tempfile::TempStdDir;

    #[test]
    fn test_cstr() {
        assert_eq!(
            std::ffi::CString::from_vec_with_nul(b"abc\0".to_vec()).unwrap(),
            cstr("abc").unwrap()
        );
    }

    #[test]
    fn test_dlerror() {
        assert_eq!("unknown dlerror".to_string(), dlerror());
    }

    #[test]
    fn test_failed_load() {
        let path = TempStdDir::default();
        let expected = format!("dlopen({}) failed: ", path.to_str().unwrap());
        assert!(dynamic_providers_from_string(
            "MYSTRING",
            &format!("myprovider:{}", path.to_str().unwrap())
        )
        .unwrap_err()
        .to_string()
        .contains(&expected));
    }
}

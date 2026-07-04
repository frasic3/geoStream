use anyhow::{anyhow, Result};
use common::{validate_password, validate_username, Message};
use db::{
    check_credentials, get_user_by_token, key_exists, save_register, try_insert_token,
};
use uuid::Uuid;

async fn create_token() -> String {
    // Keep the safety loop, but it almost always exits on the first iteration.
    loop {
        let token = Uuid::new_v4().to_string();
        if get_user_by_token(&token).await.is_none() {
            return token;
        }
    }
}

/// Performs the login. Returns (username, token).
pub async fn login(msg: &Message) -> Result<(String, String)> {
    let (username, password) = match msg {
        Message::Login { username, password } => (username.clone(), password.clone()),
        _ => return Err(anyhow!("invalid message")),
    };
    validate_username(&username).map_err(|e| anyhow!(e))?;
    validate_password(&password).map_err(|e| anyhow!(e))?;

    if !key_exists(&username).await? {
        return Err(anyhow!("invalid credentials"));
    }
    check_credentials(msg)
        .await
        .map_err(|_| anyhow!("invalid credentials"))?;

    let token = create_token().await;
    try_insert_token(&username, &token).await?;
    Ok((username, token))
}

/// Registers a new user. Returns (username, token).
pub async fn register(msg: &Message) -> Result<(String, String)> {
    let (username, password) = match msg {
        Message::Register { username, password } => (username.clone(), password.clone()),
        _ => return Err(anyhow!("invalid message")),
    };
    validate_username(&username).map_err(|e| anyhow!(e))?;
    validate_password(&password).map_err(|e| anyhow!(e))?;

    if key_exists(&username).await? {
        return Err(anyhow!("user already exists"));
    }
    save_register(msg).await?;

    let token = create_token().await;
    try_insert_token(&username, &token).await?;
    Ok((username, token))
}

// --- TESTS ---
// The tests write to a global users.json: run with --test-threads=1
// or set USERS_DB_PATH/POSITIONS_DB_PATH to unique paths.
#[cfg(test)]
mod tests {
    use super::*;
    use common::Message;
    use std::sync::{Mutex as StdMutex, Once};

    static TEST_GUARD: StdMutex<()> = StdMutex::new(());
    static TEST_DB_INIT: Once = Once::new();

    async fn setup() {
        TEST_DB_INIT.call_once(|| {
            std::fs::create_dir_all("target").ok();

            std::fs::remove_file("target/server_auth_tests.sqlite").ok();
            std::fs::remove_file("target/server_auth_tests.sqlite-shm").ok();
            std::fs::remove_file("target/server_auth_tests.sqlite-wal").ok();

            std::env::set_var(
                "DATABASE_URL",
                "sqlite://./target/server_auth_tests.sqlite",
            );
        });

        db::ensure_file_exists().await.unwrap();
        db::reset_all_for_tests().await.unwrap();
    }



    #[tokio::test]
    async fn register_new_user_returns_token() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        let (user, token) = register(&msg).await.expect("register");
        assert_eq!(user, "mario");
        assert!(!token.is_empty());

    }

    #[tokio::test]
    async fn register_duplicate_user_returns_error() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        register(&msg).await.unwrap();
        assert!(register(&msg).await.is_err());

    }

    #[tokio::test]
    async fn register_with_wrong_message_returns_error() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Login {
            username: "mario".into(),
            password: "secret".into(),
        };
        assert!(register(&msg).await.is_err());

    }

    #[tokio::test]
    async fn login_ok_returns_token() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let reg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        let (_, t0) = register(&reg).await.unwrap();
        // Simulate a "disconnect" before the re-login (single-session policy).
        db::invalidate_token(&t0).await;
        let login_msg = Message::Login {
            username: "mario".into(),
            password: "secret".into(),
        };
        let (user, token) = login(&login_msg).await.unwrap();
        assert_eq!(user, "mario");
        assert!(!token.is_empty());

    }

    #[tokio::test]
    async fn second_login_from_another_session_fails() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let reg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        register(&reg).await.unwrap();
        // Register token still active → a second login must fail.
        let login_msg = Message::Login {
            username: "mario".into(),
            password: "secret".into(),
        };
        let err = login(&login_msg).await.unwrap_err().to_string();
        assert!(
            err.contains("already logged in"),
            "expected single-session error, got: {err}"
        );

    }

    #[tokio::test]
    async fn login_nonexistent_user_returns_error() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Login {
            username: "ghost".into(),
            password: "secret".into(),
        };
        assert!(login(&msg).await.is_err());

    }

    #[tokio::test]
    async fn login_wrong_password_returns_error() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let reg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        register(&reg).await.unwrap();
        let login_msg = Message::Login {
            username: "mario".into(),
            password: "wrongpass".into(),
        };
        assert!(login(&login_msg).await.is_err());

    }

    #[tokio::test]
    async fn login_with_wrong_message_returns_error() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        assert!(login(&msg).await.is_err());

    }

    #[tokio::test]
    async fn two_different_registrations_have_different_tokens() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg1 = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        let msg2 = Message::Register {
            username: "luigi".into(),
            password: "secret".into(),
        };
        let (_, t1) = register(&msg1).await.unwrap();
        let (_, t2) = register(&msg2).await.unwrap();
        assert_ne!(t1, t2);

    }

    #[tokio::test]
    async fn token_generated_and_valid_in_session() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let msg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        let (_, token) = register(&msg).await.unwrap();
        let user = db::get_user_by_token(&token).await;
        assert_eq!(user, Some("mario".to_string()));

    }
}

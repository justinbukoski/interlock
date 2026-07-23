use sqlx::postgres::PgPoolOptions;

fn identifier(name: &str) -> Result<&str, Box<dyn std::error::Error>> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || (i > 0 && b.is_ascii_digit()));
    valid
        .then_some(name)
        .ok_or_else(|| "unsafe PostgreSQL role identifier".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("FOREMAN_V6_MIGRATION_DATABASE_URL")?;
    let migrator_name = std::env::var("FOREMAN_V6_MIGRATOR_ROLE")?;
    let runtime_name = std::env::var("FOREMAN_V6_RUNTIME_ROLE")?;
    let migrator = identifier(&migrator_name)?;
    let runtime = identifier(&runtime_name)?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    sqlx::query(&format!("SET ROLE {migrator}"))
        .execute(&pool)
        .await?;
    sqlx::migrate!().run(&pool).await?;
    for statement in [
        format!("GRANT USAGE ON SCHEMA public TO {runtime}"),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {runtime}"),
        format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {runtime}"),
        format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {runtime}"),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {migrator} IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {runtime}"
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {migrator} IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {runtime}"
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {migrator} IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {runtime}"
        ),
        format!("REVOKE CREATE ON SCHEMA public FROM {runtime}"),
    ] {
        sqlx::query(&statement).execute(&pool).await?;
    }
    Ok(())
}

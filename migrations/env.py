import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from market_execution_lab.storage import sqlalchemy_url


config = context.config
config.set_main_option("sqlalchemy.url", sqlalchemy_url(os.getenv("DATABASE_URL", config.get_main_option("sqlalchemy.url"))))


def run_migrations_offline() -> None:
    context.configure(url=config.get_main_option("sqlalchemy.url"), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(config.get_section(config.config_ini_section), prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

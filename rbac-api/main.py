import os
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException
from sqlalchemy import ARRAY, Column, String
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

DB_DSN = os.environ.get("DB_DSN", "postgresql+asyncpg://rbac:rbac@rbac-db/rbac")
engine = create_async_engine(DB_DSN, echo=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield
    # Shutdown (if needed)
    await engine.dispose()


app = FastAPI(title="RBAC API", version="0.1.0", lifespan=lifespan)


class Privilege(str, Enum):
    SELECT_TABLE = "SELECT_TABLE"
    INSERT_TABLE = "INSERT_TABLE"
    UPDATE_TABLE = "UPDATE_TABLE"
    DELETE_TABLE = "DELETE_TABLE"
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    CREATE_SCHEMA = "CREATE_SCHEMA"
    DROP_SCHEMA = "DROP_SCHEMA"
    CREATE_CATALOG = "CREATE_CATALOG"
    DROP_CATALOG = "DROP_CATALOG"
    ACCESS_CATALOG = "ACCESS_CATALOG"
    ALL = "ALL"


class Policy(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True)
    catalog: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    columns: list[str] | None = Field(
        sa_column=Column(ARRAY(String)), default=None
    )
    actions: list[Privilege] = Field(
        sa_column=Column(
            ARRAY(ENUM(Privilege, name="privilege_enum", create_type=False))
        ),
        default_factory=lambda: [Privilege.SELECT_TABLE],
    )


class UserPolicyView(SQLModel):
    catalog: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    columns: list[str] | None = None
    actions: list[str]


@app.get("/users/{username}/policies", response_model=list[UserPolicyView])
async def get_user_policies(username: str):
    async with AsyncSession(engine) as session:
        result = await session.exec(
            select(Policy).where(Policy.username == username)
        )
        policies = result.all()
        if not policies:
            raise HTTPException(status_code=404, detail="user not found")
        return policies

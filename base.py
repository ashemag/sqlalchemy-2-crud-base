import typing as t

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.config.logger import get_logger
from app.db.base_class import Base
from app.utils.utils import camel_to_snake

logger = get_logger(__name__)


class CRUDBase:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    async def get(self, model: t.Type[Base], id: t.Any) -> t.Optional[Base]:
        return await self.db.get(model, id)

    async def get_by_field(
        self, model: t.Type[Base], field_name: str, field_value: t.Any
    ) -> t.Optional[Base]:
        field = getattr(model, field_name)
        stmt = select(model).where(field == field_value)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_multi_by_field(
        self,
        model: t.Type[Base],
        field_name: str,
        field_value: t.Any,
        limit: int | None = None,
    ) -> t.Optional[t.List[Base]]:
        field = getattr(model, field_name)
        stmt = select(model).where(field == field_value)

        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def create(
        self, model: t.Type[Base], obj_in: t.Union[BaseModel, t.Dict[str, t.Any]]
    ) -> BaseModel:
        if isinstance(obj_in, BaseModel):
            obj_in_data = jsonable_encoder(obj_in)
            db_obj = model(**camel_to_snake(obj_in_data))  # type: ignore
        elif isinstance(obj_in, t.Dict):
            db_obj = model(**obj_in)
        else:
            raise ValueError("Invalid input type for obj_in")

        self.db.add(db_obj)
        await self.db.commit()
        await self.db.refresh(db_obj)
        return db_obj

    async def delete(self, model: t.Type[Base], id: str) -> None:
        obj = await self.db.get(model, id)
        if obj:
            await self.db.delete(obj)
            await self.db.commit()

    async def create_many(
        self,
        model: t.Type[Base],
        obj_in_list: t.List[t.Union[BaseModel, t.Dict[str, t.Any]]],
    ) -> t.List[BaseModel]:
        created_objects = []
        for obj_in in obj_in_list:
            if isinstance(obj_in, BaseModel):
                obj_in_data = jsonable_encoder(obj_in)
                db_obj = model(**camel_to_snake(obj_in_data))  # type: ignore
            elif isinstance(obj_in, t.Dict):
                db_obj = model(**obj_in)
            else:
                raise ValueError("Invalid input type for obj_in")

            self.db.add(db_obj)
            created_objects.append(db_obj)

        await self.db.commit()

        for obj in created_objects:
            await self.db.refresh(obj)

        return created_objects

    async def delete_many(self, model: t.Type[Base], ids: t.List[str]) -> None:
        for _id in ids:
            obj = await self.db.get(model, _id)
            if obj:
                await self.db.delete(obj)
        await self.db.commit()

    async def delete_by_field(
        self, model: t.Type[Base], field_name: str, field_value: t.Any
    ) -> None:
        records = await self.db.execute(
            select(model).where(getattr(model, field_name) == field_value)
        )
        records = records.scalars().all()

        for record in records:
            await self.db.delete(record)

        await self.db.commit()

    async def update_many(
        self,
        model: t.Type[Base],
        field_name: str,
        field_value: t.Any,
        update_values: t.Dict[str, t.Any],
    ) -> t.Union[Base, t.List[Base]]:
        result = await self.db.execute(
            select(model).where(getattr(model, field_name) == field_value)
        )
        records = result.scalars().all()

        for record in records:
            for key, value in update_values.items():
                if hasattr(record, key):
                    setattr(record, key, value)

        await self.db.commit()
        return records[0] if len(records) == 1 else records

    async def upsert(
        self, model: t.Type[Base], obj_in: t.Union[BaseModel, t.Dict[str, t.Any]]
    ) -> Base:
        obj_in_data = jsonable_encoder(obj_in)

        existing_record = await self.db.get(model, obj_in_data.get("id"))
        if existing_record:
            # Update the existing record
            for key, value in obj_in_data.items():
                if hasattr(existing_record, key):
                    setattr(existing_record, key, value)
            self.db.add(existing_record)
        else:
            # Create a new record
            new_record = model(**obj_in_data)
            self.db.add(new_record)

        await self.db.commit()
        return existing_record if existing_record else new_record

    async def upsert_many(
        self,
        model: t.Type[Base],
        obj_in_list: t.List[t.Union[BaseModel, t.Dict[str, t.Any]]],
    ) -> t.List[Base]:
        updated_or_created_records = []
        for obj_in in obj_in_list:
            obj_in_data = jsonable_encoder(obj_in)

            existing_record = await self.db.get(model, obj_in_data.get("id"))
            if existing_record:
                # Update the existing record
                for key, value in obj_in_data.items():
                    if hasattr(existing_record, key):
                        setattr(existing_record, key, value)
                self.db.add(existing_record)
                updated_or_created_records.append(existing_record)
            else:
                # Create a new record
                new_record = model(**obj_in_data)
                self.db.add(new_record)
                updated_or_created_records.append(new_record)

        await self.db.commit()
        return updated_or_created_records

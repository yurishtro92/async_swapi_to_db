import aiohttp
from more_itertools import chunked
import requests
from typing import Iterable
from sqlalchemy import Integer, String, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import config
from sqlalchemy import insert
import asyncio
from aiopg.sa import create_engine

# -----------------------выгрузка из API-------------------------
BOTTLE_NECK = 10
resp = requests.get('https://swapi.dev/api/people/')  # quantity - число всех персонажей в API
quantity = (resp.json()['count'])


async def get_persons(session: aiohttp.client.ClientSession, range_person_id: Iterable[int]):
    persons_list = []
    for person_id in range_person_id:
        res = await get_person(session, person_id)
        persons_list.append(res)
    for person_id_chunk in chunked(range_person_id, BOTTLE_NECK):
        get_person_tasks = [asyncio.create_task(get_person(session, person_id)) for person_id in person_id_chunk]
        await asyncio.gather(*get_person_tasks)
    return persons_list  # возвращается список всех персонажей, имеющихся в API


async def get_person(session: aiohttp.client.ClientSession, person_id: int) -> dict:
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        response_json = await response.json()
        try:
            response_json['id'] = person_id
            response_json.pop('created')
            response_json.pop('edited')
            response_json.pop('url')
        except KeyError:
            response_json['id'] = person_id
        return response_json  # возвращается один персонаж из API


# --------------------создание БД----------------------
engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


class persons(Base):  # описание таблицы в БД
    __tablename__ = 'persons'
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    birth_year = Column(String, index=True)
    eye_color = Column(String, index=True)
    films = Column(String, index=True)
    gender = Column(String, index=True)
    hair_color = Column(String, index=True)
    height = Column(Integer)
    homeworld = Column(String, index=True)
    mass = Column(Integer)
    skin_color = Column(String, index=True)
    species = Column(String, index=True)
    starships = Column(String, index=True)
    vehicles = Column(String, index=True)


# создание БД
async def get_async_session(
        drop: bool = False, create: bool = False
):
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print(1)
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    return async_session_maker


# -------------------выгрузка в БД---------------------
async def add_person(person):  # запись одной строки в БД
    async with create_engine(config.PG_DSN) as engine:
        async with engine.acquire() as conn:
            try:
                req = insert(persons).values(id=person['id'], name=person['name'], height=int(person['height']),
                                             mass=int(person['mass']),
                                             hair_color=person['hair_color'], skin_color=person['skin_color'],
                                             eye_color=person['eye_color'], birth_year=person['birth_year'],
                                             gender=person['gender'], homeworld=person['homeworld'],
                                             films=person['films'], species=person['species'],
                                             vehicles=person['vehicles'], starships=person['starships'])
                await conn.execute(req)
            except KeyError:  # обработка возможных ошибок при некорректных данных в API
                req = insert(persons).values(id=person['id'], name='unknown', height=0,
                                             mass=0,
                                             hair_color='unknown', skin_color='unknown',
                                             eye_color='unknown', birth_year='unknown',
                                             gender='unknown', homeworld='unknown',
                                             films='unknown', species='unknown',
                                             vehicles='unknown', starships='unknown')

            except ValueError:  # обработка возможных ошибок при некорректных данных в API
                req = insert(persons).values(id=person['id'], name='unknown', height=0,
                                             mass=0,
                                             hair_color='unknown', skin_color='unknown',
                                             eye_color='unknown', birth_year='unknown',
                                             gender='unknown', homeworld='unknown',
                                             films='unknown', species='unknown',
                                             vehicles='unknown', starships='unknown')


async def add_persons():  # запись всех строк в БД
    async with aiohttp.client.ClientSession() as session:
        persons_list = await get_persons(session, range(1, quantity))
        for person in persons_list:
            await add_person(person)


async def main():
    async with aiohttp.client.ClientSession() as session:
        await get_persons(session, range(1, quantity))
        await get_async_session(True, True)
        await add_persons()


if __name__ == '__main__':
    asyncio.run(main())

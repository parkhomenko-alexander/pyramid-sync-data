
from datetime import datetime
from typing import Sequence

from sqlalchemy import ARRAY, TIMESTAMP, String, INTEGER, and_, bindparam, select, text, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.data import Data
from app.repositories.abstract_repository import SQLAlchemyRepository
from app.schemas.data_schema import DataAddSheme


class DataRepository(SQLAlchemyRepository[Data]):
    def __init__(self, async_session: AsyncSession):
        super().__init__(async_session, Data)

    async def get_existing_values(self, values: set[DataAddSheme]) -> Sequence[Data]:
        records = [(value.created_at, value.tag_id, value.device_sync_id) for value in values]

        stmt = (
            select(self.model).
            where(
                tuple_(self.model.created_at, self.model.tag_id, self.model.device_sync_id).in_(records)
            )
        )

        res = await self.async_session.execute(stmt)

        return res.scalars().all()


    async def bulk_update_by_external_ids(self, data: list[dict]) -> int:
        for item in data:
            stmt = (
                update(self.model).
                where((self.model.created_at == item["created_at"]) & (self.model.tag_id == item["tag_id"]) & (self.model.device_sync_id == item["device_sync_id"])).
                values(**item)
            )
            
            await self.async_session.execute(stmt)
         
        return 0
    
    async def get_device_data(self, device_sync_id: int, time_range: list[str], tag_id: int):
        stmt = (
            select(
                self.model.value/1000000, self.model.created_at
            ).
            where(
                and_(
                    self.model.created_at.between(time_range[0], time_range[1]),
                    self.model.device_sync_id==device_sync_id,
                    self.model.tag_id == tag_id
                )
            )
        )

        res = await self.async_session.execute(stmt)

        return res
    
    async def get_data_buildings(self, start: datetime, end: datetime, devices: dict[str, str]):
        cases = []
        for d, alias in devices.items():
            cases.append(
                f"MAX(CASE WHEN d.full_title = '{d}' THEN dt.value/1000000  END) AS \"{alias}\""
            )

        pivot_sql = ",\n    ".join(cases)

        stmt = text(
            f"""
                WITH time_slots as (
                    SELECT generate_series(
                        :start,
                        :end,
                        INTERVAL '30 minutes'
                    ) as timestamp
                )

                SELECT
                    ts.timestamp,
                    {pivot_sql}
                FROM
                    time_slots ts
                LEFT JOIN data dt
                    ON ts.timestamp = dt.created_at
                LEFT JOIN devices d
                    ON dt.device_sync_id = d.sync_id
                WHERE d.full_title = ANY(:device_titles)
                GROUP BY
                    ts.timestamp
                ORDER BY
                    ts.timestamp;
            """
        ).bindparams(
            bindparam("start", type_=TIMESTAMP),
            bindparam("end", type_=TIMESTAMP),
            bindparam("device_titles", type_=ARRAY(String))
        )

        params = {
            "start": start,
            "end": end,
            "device_titles": list(devices.keys())
        }

        result = await self.async_session.execute(
            stmt,
            params
        )
        r = result.mappings().all()
        return r


    async def get_data_cg(self, start: datetime, end: datetime):
        stmt = text(
            f"""
                WITH DevicesByGroup AS (
                    SELECT
                        d.sync_id,
                    CASE
                        -- 'Вентиляция'
                        WHEN d.sync_id IN (
                            '51904','52066','52072','52084','52090','61329','9923','9953','9959','9971',
                            '10043','10061','10067','10079','10085','10097','10103',
                            '36193','36229','36301','36061','36097','36145','36163','36181',
                            '8272','8314','8496','7078','7132','7150',
                            '35554','35584','35668','6757','6811','6829','6334','6352','6388','6436',
                            '6475','6493','6529','6595','6613','6649','50289','50325','50349','50379','50439','50463'
                        ) THEN 'Вентиляция'
                        
                        -- 'ИТП'
                        WHEN d.sync_id IN (
                            '51934','51940','61323','9941','9947','36307','36313','8338','8344',
                            '8508','8514','7168','7174','7198','7204','6853','6877','6883','49985',
                            '6406','6412','6547','6553','6667','6673','50469','50475'
                        ) THEN 'ИТП'

                        -- Розетки
                        WHEN d.sync_id IN (
                            '51880','51898','51946','51982','52000','52012','52030','52054','52060',
                            '52108','61323',
                            '9887','9893','9905','9989','10007','10025','10115',
                            '36211','36247','36253','36259','36265','36277','36283','36295',
                            '36079','36097','36139','36157','36175',
                            '8290','8308','8326','8332','8490',
                            '7084','7102','7114','7120','7138','7186',
                            '35560','35638','35650','35656','35674','35692',
                            '6763','6781','6793','6799','6817','6865',
                            '6340','6370','6394','6418','6442',
                            '6481','6511','6535','6559',
                            '6601','6631','6655','6679',
                            '50295','50307','50313','50337','50343','50367','50373','50397',
                            '50403','50409','50415','50427','50445','50457'
                        ) THEN 'Розетки'

                        -- Освещение
                        WHEN d.sync_id IN (
                            '51886','51952','51976','52018','52036','52102',
                            '9911','9977','9995','10013','10031','10049','10121',
                            '36199','36217','36235','36067','36085','36103','36121',
                            '8278','8296','8478',
                            '7096','7156','7180','35590','35632','35686',
                            '6775','6835','6859','6358','6376','6424',
                            '6499','6517','6565','6619','6637','6685',
                            '50319','50355','50385','50421'
                        ) THEN 'Освещение'

                        -- Пожарка
                        WHEN d.sync_id IN (
                            '51892','51910','51928','52114','51958','51988','52024','52042','52048','52078','52096',
                            '9899','9917','9935','9965','10001','10019','10037','10055','10073','10091','10109',
                            '36205','36223','36241','36271','36289',
                            '36073','36091','36109','36151','36169','36187',
                            '8284','8302','8320','8484','8502',
                            '7090','7108','7126','7144','7162','7192',
                            '35566','35596','35644','35662','35680','35698',
                            '6841','6769','6787','6805','6823','6871','49979',
                            '6346','6364','6382','6400','6430','6448',
                            '6487','6505','6523','6541','6571','6589',
                            '6607','6625','6643','6661','6691','6709',
                            '50301','50331','50361','50391','50433','50451'
                        ) THEN 'Пожарка'

                        -- 'Столовая'
                        WHEN d.sync_id IN (
                            '51916','51922','9929','36127','36133','8472',
                            '35572','35578','35614','35620','35716','35722','35734','35740',
                            '6577','6583'
                        ) THEN 'Столовая'


                        -- Фитнес
                        WHEN d.sync_id IN ('6697','6703') THEN 'Фитнес'

                        -- Мед оборудование
                        WHEN d.sync_id IN ('51964','51970') THEN 'МедОборудование'

                        -- Операционная
                        WHEN d.sync_id IN ('52006') THEN 'Операционная'

                    END AS group_alias
                    FROM
                        devices d
                ),

                time_slots as (
                    SELECT generate_series(
                        :start,
                        :end,
                        INTERVAL '30 minutes'
                    ) as timestamp
                )

                SELECT
                ts.timestamp,
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Вентиляция'),0)/1000000 AS "Вентиляция",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'ИТП'),0)/1000000 AS "ИТП",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Розетки'),0)/1000000 AS "Розетки",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Освещение'),0)/1000000 AS "Освещение",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Пожарка'),0)/1000000 AS "Пожарка",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Столовая'),0)/1000000 AS "Столовая",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Фитнес'),0)/1000000 AS "Фитнес",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'МедОборудование'),0)/1000000 AS "МедОборудование",
                COALESCE(SUM(dt.value) FILTER (WHERE dbg.group_alias = 'Операционная'),0)/1000000 AS "Операционная"

                FROM
                    time_slots ts
                LEFT JOIN data dt
                    ON ts.timestamp = dt.created_at
                LEFT JOIN DevicesByGroup dbg
                    ON dt.device_sync_id = dbg.sync_id
                WHERE
                    dbg.group_alias IS NOT NULL
                GROUP BY
                    ts.timestamp
                ORDER BY
                    ts.timestamp;
            """
        ).bindparams(
            bindparam("start", type_=TIMESTAMP),
            bindparam("end", type_=TIMESTAMP)
        )

        params = {
            "start": start,
            "end": end,
        }

        result = await self.async_session.execute(
            stmt,
            params
        )
        r = result.mappings().all()
        return r

    async def get_data_for_specific_devices(
        self,
        start: datetime,
        end: datetime,
        # sync_ids: list[int],
        groups: list[tuple[int, str]],
        tag: str
    ):
        values_sql = ", ".join(
            f"({sync_id}, '{group_name}')" 
            for sync_id, group_name in groups
        )
        stmt = text(
            f"""
                WITH groups(device_sync_id, group_name) AS (
                    VALUES {values_sql}
                )

                SELECT
                    g.group_name,
                    d.created_at,
                    SUM(d.value) / 1000000 AS value
                FROM data d
                JOIN groups g ON g.device_sync_id = d.device_sync_id
                WHERE d.created_at BETWEEN :start AND :end
                AND d.tag_id = (SELECT id FROM tags WHERE title = :tag)
                GROUP BY g.group_name, d.created_at
                ORDER BY g.group_name, d.created_at;
            """
        ).bindparams(
            bindparam("start", start, type_=TIMESTAMP),
            bindparam("end", end, type_=TIMESTAMP),
            bindparam("tag", tag, type_=String)
        )

        res = await self.async_session.execute(stmt)

        return res.mappings().all()


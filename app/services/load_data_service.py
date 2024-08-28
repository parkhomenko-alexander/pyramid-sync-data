import os
from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from app.services.helper import with_uow
from app.utils.unit_of_work import AbstractUnitOfWork
from config import config


class LoadDataFromFilesService():
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow: AbstractUnitOfWork = uow

    def last_day_of_month(self, date):
        if date.month == 12:
            return date.replace(day=31, hour=23, minute=0, second=0)
        return date.replace(month=date.month+1, day=1, hour=23, minute=0, second=0) - timedelta(days=1)
    
    def parse_number(self, input_value):
        if str(input_value) == "nan":
                return 0
        if isinstance(input_value, int):
            return float(input_value)
        if isinstance(input_value, float):
            return input_value
        elif isinstance(input_value, str):
            # Remove spaces (thousand separators)
            no_spaces = input_value.replace(' ', '')
            # Replace comma (decimal separator) with period
            standard_decimal = no_spaces.replace(',', '.')
            # Convert to float
            return float(standard_decimal)
        else:
            raise ValueError("Input must be either a string or a float")

    @with_uow
    async def insert_from_file(self, table_name: str) -> int:
        try:
            year = 2024

            file_path: str = config.APP_DATA_DIR + f"/{table_name}.xlsx"
            df = pd.read_excel(file_path)

            for row in df.itertuples(index=False):
                unit = row[0]
                for i in range(1, len(row), 6):
                        month = (i - 1) // 6 + 1
                        last_day_timestamp = self.last_day_of_month(datetime(year, month, 1))

                        group_of_six = row[i:i+6]
                        item = {
                            "electro_unit": unit,
                            "created_at": last_day_timestamp,

                            "plan": self.parse_number(group_of_six[0]),
                            "fact": self.parse_number(group_of_six[2]),

                            "plane_coast": self.parse_number(group_of_six[1]),
                            "fact_coast": self.parse_number(group_of_six[3]),

                            "delta_values": self.parse_number(group_of_six[4]),
                            "delta_coast": self.parse_number(group_of_six[5]),
                        }

                        if table_name == "electro":
                            await self.uow.electro_repo.add(item)
                        if table_name == "water":
                            await self.uow.water_repo.add(item)
                        if table_name == "warm":
                            await self.uow.warm_repo.add(item)

            await self.uow.commit()
            logger.info(f"{table_name} loaded")
            return 0
        except Exception as er:
            logger.error(er)
            return 1
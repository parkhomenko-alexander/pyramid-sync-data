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

            no_spaces = input_value.replace(' ', '').replace('\xa0', '')
            # Replace comma (decimal separator) with period
            standard_decimal = no_spaces.replace(',', '.')
            # Convert to float
            return float(standard_decimal)
        else:
            raise ValueError("Input must be either a string or a float")

    @with_uow
    async def insert_from_file(self, table_name: str) -> int:
        try:
            base_year = datetime.now().year
            start_month = 2

            file_path: str = config.APP_DATA_DIR + f"/{table_name}.xlsx"
            df = pd.read_excel(file_path)

            for row in df.itertuples(index=False):
                unit = row[0]  # First column is the unit name

                # Iterate through the row in groups of 6 columns
                for i in range(1, len(row), 6):
                    # Calculate the month based on the column index
                    
                    total_month_offset = ((i - 1) // 6)
                    month = (start_month + total_month_offset - 1) % 12 + 1
                    year = base_year + ((start_month + total_month_offset - 1) // 12)

                    # Get the last day of the month for the calculated year
                    last_day_timestamp = self.last_day_of_month(datetime(year, month, 1))

                    # Extract the group of 6 columns
                    group_of_six = row[i:i + 6]

                    # Skip if the group doesn't have exactly 6 columns
                    if len(group_of_six) != 6:
                        logger.warning(f"Skipping incomplete group for unit {unit}, month {month}, year {year}")
                        continue

                    # Create the item dictionary
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
                    print(item)

                    # Insert the item into the appropriate table
                    if "electro" in table_name:
                        await self.uow.electro_repo.add(item)
                    elif "water" in table_name:
                        await self.uow.water_repo.add(item)
                    elif "warm" in table_name:
                        await self.uow.warm_repo.add(item)

            # Commit the transaction
            await self.uow.commit()
            logger.info(f"{table_name} loaded successfully")
            return 0

        except Exception as er:
            logger.exception(f"Error loading {table_name}: {er}")
            return 1
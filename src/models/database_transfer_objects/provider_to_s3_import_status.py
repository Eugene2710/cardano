from datetime import datetime
from pydantic import BaseModel


class ProviderToS3ImportStatusDTO(BaseModel):
    table: str
    block_height: int
    created_at: datetime

    @staticmethod
    def create_import_status(table: str, block_height: int) -> "ProviderToS3ImportStatusDTO":
        return ProviderToS3ImportStatusDTO(
            table=table,
            block_height=block_height,
            created_at=datetime.utcnow(),
        )

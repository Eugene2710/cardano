from datetime import datetime
from pydantic import BaseModel


class S3ToDBImportStatusDTO(BaseModel):
    table: str
    block_height: int
    created_at: datetime

    @staticmethod
    def create_import_status(table: str, block_height: int) -> "S3ToDBImportStatusDTO":
        return S3ToDBImportStatusDTO(
            table=table,
            block_height=block_height,
            created_at=datetime.utcnow(),
        )


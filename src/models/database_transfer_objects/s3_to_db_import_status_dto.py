from datetime import datetime
from pydantic import BaseModel


class S3ToDBImportStatusDTO(BaseModel):
    table: str
    file_modified_date: datetime
    created_at: datetime

    @staticmethod
    def create_import_status(table: str, file_modified_date: datetime) -> "S3ToDBImportStatusDTO":
        return S3ToDBImportStatusDTO(
            table=table,
            file_modified_date=file_modified_date,
            created_at=datetime.utcnow(),
        )


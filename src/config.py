from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    eia_datasets: str = "STEO,TOTAL,INTL,SEDS,IEO"
    eia_db_path: str = "data/eia_bulk.duckdb"
    eia_data_dir: str = "data"
    eia_force_reload: bool = False
    eia_manifest_url: str = "https://www.eia.gov/opendata/bulk/manifest.txt"
    eia_batch_size: int = 50000
    host: str = "0.0.0.0"
    port: int = 7779

    model_config = {"env_prefix": "", "case_sensitive": False}

    @property
    def dataset_list(self) -> list[str]:
        val = self.eia_datasets.strip()
        if val.upper() == "ALL":
            return []
        return [d.strip() for d in val.split(",") if d.strip()]


settings = Settings()

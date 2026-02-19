"""
Ingestion Module - 数据灌入模块
"""
from modules.ingestion.akshare_client import akshare_client, AkShareClient
from modules.ingestion.sync_asset import (
    asset_syncer,
    sync_assets,
)
from modules.ingestion.sync_concept import (
    concept_syncer,
    sync_concepts,
    sync_concept_constituents,
)
from modules.ingestion.sync_industry import (
    industry_syncer,
    sync_industries,
    sync_industry_constituents,
)
from modules.ingestion.sync_profile import (
    profile_syncer,
    sync_profile,
    sync_profiles,
    search_companies,
)

__all__ = [
    # AkShare Client
    "akshare_client",
    "AkShareClient",
    # Asset Syncer
    "asset_syncer",
    "sync_assets",
    # Concept Syncer
    "concept_syncer",
    "sync_concepts",
    "sync_concept_constituents",
    # Industry Syncer
    "industry_syncer",
    "sync_industries",
    "sync_industry_constituents",
    # Profile Syncer
    "profile_syncer",
    "sync_profile",
    "sync_profiles",
    "search_companies",
]

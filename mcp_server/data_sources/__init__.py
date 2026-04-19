"""
Data Sources Package

Contains data access classes for different astronomical datasets.
"""

from .base import BaseDataSource
from .astroquery_universal import AstroqueryUniversal

__all__ = ['BaseDataSource', 'AstroqueryUniversal'] 
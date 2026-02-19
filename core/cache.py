"""
Cache 缓存管理 - DiskCache 封装

用于缓存 AkShare API 调用结果，防止频繁请求被封 IP
"""
import logging
import hashlib
import pickle
from typing import Any, Callable, Optional, TypeVar
from functools import wraps

from diskcache import Cache

from config.settings import CACHE_DIR, CACHE_TTL_SECONDS, CACHE_SIZE_LIMIT

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CacheManager:
    """
    缓存管理器 - 单例模式
    
    使用 DiskCache 实现磁盘缓存
    """
    _instance: Optional["CacheManager"] = None
    _initialized: bool = False
    
    def __new__(cls) -> "CacheManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # 确保缓存目录存在
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # 初始化 DiskCache
        self._cache = Cache(
            directory=str(CACHE_DIR),
            size_limit=CACHE_SIZE_LIMIT,
        )
        
        self._initialized = True
        logger.info(f"CacheManager initialized: {CACHE_DIR}")
    
    @property
    def cache(self) -> Cache:
        """获取 DiskCache 实例"""
        return self._cache
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        value = self._cache.get(key)
        if value is not None:
            logger.debug(f"Cache hit: {key}")
        return value
    
    def set(self, key: str, value: Any, ttl: int = CACHE_TTL_SECONDS) -> None:
        """设置缓存值"""
        self._cache.set(key, value, expire=ttl)
        logger.debug(f"Cache set: {key} (ttl={ttl}s)")
    
    def delete(self, key: str) -> bool:
        """删除缓存值"""
        result = self._cache.delete(key)
        if result:
            logger.debug(f"Cache deleted: {key}")
        return result
    
    def clear(self) -> None:
        """清空所有缓存"""
        self._cache.clear()
        logger.info("Cache cleared")
    
    def stats(self) -> dict:
        """获取缓存统计信息"""
        return {
            "size": len(self._cache),
            "volume": self._cache.volume(),
        }
    
    def close(self) -> None:
        """关闭缓存"""
        self._cache.close()
        logger.info("Cache closed")


# 全局单例
cache_manager = CacheManager()


def make_cache_key(prefix: str, *args, **kwargs) -> str:
    """
    生成缓存 key
    
    Args:
        prefix: 缓存前缀
        *args: 位置参数
        **kwargs: 关键字参数
    
    Returns:
        缓存 key
    """
    # 将参数序列化为字符串
    key_parts = [prefix]
    
    for arg in args:
        if arg is not None:
            key_parts.append(str(arg))
    
    for k, v in sorted(kwargs.items()):
        if v is not None:
            key_parts.append(f"{k}={v}")
    
    key_str = "_".join(key_parts)
    
    # 如果 key 太长，使用 hash
    if len(key_str) > 200:
        hash_suffix = hashlib.md5(key_str.encode()).hexdigest()[:16]
        key_str = f"{prefix}_{hash_suffix}"
    
    return key_str


def cached(
    prefix: str,
    ttl: int = CACHE_TTL_SECONDS,
    key_func: Optional[Callable[..., str]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    缓存装饰器
    
    Args:
        prefix: 缓存 key 前缀
        ttl: 缓存过期时间(秒)
        key_func: 自定义 key 生成函数
    
    Example:
        @cached("ak_spot", ttl=60)
        def get_stock_spot(symbol: str) -> pd.DataFrame:
            return ak.stock_zh_a_spot_em()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # 生成缓存 key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = make_cache_key(prefix, *args, **kwargs)
            
            # 尝试从缓存获取
            cached_value = cache_manager.get(cache_key)
            if cached_value is not None:
                return cached_value
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 缓存结果
            if result is not None:
                cache_manager.set(cache_key, result, ttl=ttl)
            
            return result
        
        return wrapper
    return decorator


# 便捷函数
def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    return cache_manager.get(key)


def set_cache(key: str, value: Any, ttl: int = CACHE_TTL_SECONDS) -> None:
    """设置缓存"""
    cache_manager.set(key, value, ttl)


def delete_cache(key: str) -> bool:
    """删除缓存"""
    return cache_manager.delete(key)


def clear_cache() -> None:
    """清空缓存"""
    cache_manager.clear()

import os
import sqlite3
import time

import pandas as pd
import requests

from src.utils.log import get_logger

# 确保引入你的时区配置
try:
    from config.loader import TIMEZONE
except ImportError:
    TIMEZONE = "+8"  # 兜底默认值

logger = get_logger(__name__)


class OKXDataLoader:
    BATCH_SAVE_SIZE = 10000  # 每获取10000根K线保存一次

    def __init__(self, symbol="ETH-USDT-SWAP", timeframe="1H", db_dir=None):
        self.symbol = symbol
        self.timeframe = timeframe
        self.base_url = "https://www.okx.com"

        self.session = requests.Session()

        self.bar_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1H': '1H',
            '4H': '4H',
            '1D': '1D'
        }
        if timeframe not in self.bar_map:
            raise IndexError(f"没有这个timeframe: {timeframe}")
        self.okx_bar = self.bar_map.get(timeframe)

        # ==========================================
        # 核心修改：利用 __file__ 动态获取项目根目录
        # ==========================================
        if db_dir is None:
            # 获取 okx_loader.py 的绝对路径
            current_file = os.path.abspath(__file__)
            # 向上推三层：okx_loader.py -> data_feed -> src -> 根目录 (Momentum1.66)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
            # 强行把数据库目录锁定在项目根目录下的 data 文件夹里
            db_dir = os.path.join(project_root, 'data')

        if not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.db_path = os.path.join(db_dir, 'crypto_history.db')
        self.table_name = f"{symbol.replace('-', '_')}_{timeframe}"

    def _get_db_connection(self):
        return sqlite3.connect(self.db_path)

    def _get_current_local_time(self):
        """获取带有配置时区偏移的当前时间"""
        now_utc = pd.Timestamp.utcnow().tz_localize(None)
        if "+" in TIMEZONE:
            now_utc += pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
        elif "-" in TIMEZONE:
            now_utc -= pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))
        return now_utc

    def load_local_data(self) -> pd.DataFrame:
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.table_name}'")
            if cursor.fetchone()[0] == 0:
                conn.close()
                return pd.DataFrame()

            df = pd.read_sql(f"SELECT * FROM {self.table_name}", conn, index_col='timestamp', parse_dates=['timestamp'])
            conn.close()
            return df
        except Exception as e:
            logger.error(f"读取本地数据库失败: {e}")
            return pd.DataFrame()

    def save_local_data(self, df: pd.DataFrame):
        if df.empty:
            return
        conn = self._get_db_connection()
        df.to_sql(self.table_name, conn, if_exists='replace', index=True)
        conn.close()
        logger.debug(f"💾 成功将 {len(df)} 根 K 线保存至本地数据库: [{self.table_name}]")

    def _append_to_local_data(self, df: pd.DataFrame):
        """
        将新数据追加到本地数据库，合并去重

        Args:
            df: 新的K线数据
        """
        if df.empty:
            return

        # 加载现有数据
        existing_df = self.load_local_data()

        if existing_df.empty:
            # 没有现有数据，直接保存
            self.save_local_data(df)
            logger.debug(f"💾 追加数据: 无现有数据，直接保存 {len(df)} 根K线")
        else:
            # 合并数据，去重，按时间排序
            combined_df = pd.concat([existing_df, df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index(ascending=True)

            # 保存合并后的数据
            self.save_local_data(combined_df)
            logger.debug(f"💾 追加数据: 合并现有 {len(existing_df)} 根和新 {len(df)} 根，去重后 {len(combined_df)} 根K线")

    def _save_candles_batch(self, candles_batch: list):
        """
        保存一批原始蜡烛数据到本地数据库

        Args:
            candles_batch: 原始蜡烛数据列表
        """
        if not candles_batch:
            return

        # 转换为DataFrame
        df = pd.DataFrame(candles_batch,
                          columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volCcy', 'volCcyQuote',
                                   'confirm'])
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)

        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms')
        if "+" in TIMEZONE:
            df['timestamp'] += pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
        elif "-" in TIMEZONE:
            df['timestamp'] += pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))

        df.sort_values('timestamp', ascending=True, inplace=True)
        df.set_index('timestamp', inplace=True)

        # 追加到本地数据库
        self._append_to_local_data(df)
        logger.debug(f"💾 保存批次数据: {len(candles_batch)} 根蜡烛，时间范围 {df.index[0]} -> {df.index[-1]}")

    def fetch_from_okx(self, limit=100, after_ts=None, max_retries=10) -> pd.DataFrame:
        """原生调用 OKX V5 接口拉取历史 K 线 (自动分批防封版)"""
        endpoint = "/api/v5/market/history-candles"
        url = f"{self.base_url}{endpoint}"

        all_candles = []
        batch_candles = []
        current_after = after_ts
        batch_size_threshold = 5000

        logger.info(f"开始通过原生 API 批量拉取 {self.symbol} {self.timeframe} 数据，目标 {limit} 根...")

        while len(all_candles) < limit:
            fetch_size = min(100, limit - len(all_candles))
            params = {
                "instId": self.symbol,
                "bar": self.okx_bar,
                "limit": fetch_size
            }
            if current_after:
                params["after"] = current_after

            candles = []
            success = False

            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json()

                    if data.get("code") != "0":
                        raise ValueError(f"OKX 业务报错: {data.get('msg')}")

                    candles = data.get("data", [])
                    if not candles:
                        success = True
                        break

                    all_candles.extend(candles)
                    batch_candles.extend(candles)
                    current_after = candles[-1][0]
                    success = True

                    # 检查批次大小，如果达到阈值则保存
                    if len(batch_candles) >= self.BATCH_SAVE_SIZE:
                        self._save_candles_batch(batch_candles)
                        batch_candles = []

                    if len(all_candles) % 10000 == 0 or len(all_candles) == limit:
                        logger.info(f"拉取进度: {len(all_candles)} / {limit} ...")

                    break

                except Exception as e:
                    logger.warning(
                        f"网络颠簸 (进度 {len(all_candles)}/{limit}) | 第 {attempt + 1}/{max_retries} 次重试... 报错: {e}")
                    self.session.close()
                    self.session = requests.Session()
                    sleep_time = 3 + (attempt * 2)
                    time.sleep(sleep_time)

            if not success or not candles:
                logger.error(f"严重网络故障或无更多数据。停止拉取！将返回已成功获取的 {len(all_candles)} 根数据。")
                break

            if len(all_candles) > 0 and len(all_candles) % batch_size_threshold == 0:
                logger.info(f"🟢 已完成一个大批次 ({len(all_candles)}根)，强制休眠 3 秒，防封锁...")
                time.sleep(3)
            else:
                time.sleep(0.15)

        # 保存剩余的批次数据
        if batch_candles:
            self._save_candles_batch(batch_candles)
            batch_candles = []

        if not all_candles:
            return pd.DataFrame()

        df = pd.DataFrame(all_candles,
                          columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volCcy', 'volCcyQuote',
                                   'confirm'])
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)

        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='ms')
        if "+" in TIMEZONE:
            df['timestamp'] += pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
        elif "-" in TIMEZONE:
            df['timestamp'] += pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))

        df.sort_values('timestamp', ascending=True, inplace=True)
        df.set_index('timestamp', inplace=True)

        current_time = self._get_current_local_time()
        if not df.empty and (current_time - df.index[-1]).total_seconds() < self._get_seconds(self.timeframe):
            df = df.iloc[:-1]

        return df

    def _fetch_historical_data_with_limit(self, limit=50000):
        """
        内部方法：使用现有逻辑拉取指定数量的K线
        这是原 fetch_historical_data 的核心逻辑，但不包含日期范围过滤
        """
        logger.debug(f"🔍 准备加载 {self.symbol} ({self.timeframe}) 数据...")
        local_df = self.load_local_data()

        if local_df.empty:
            logger.info(f"⚠️ 本地无数据，将从 OKX 全量拉取 {limit} 根...")
            final_df = self.fetch_from_okx(limit=limit)
            self.save_local_data(final_df)
            return final_df.tail(limit)

        local_count = len(local_df)
        last_local_time = local_df.index[-1]
        oldest_local_time = local_df.index[0]
        logger.debug(f"📦 本地数据库已命中！现有 {local_count} 根 K 线 | 区间: {oldest_local_time} -> {last_local_time}")

        current_local = self._get_current_local_time()
        bar_seconds = self._get_seconds(self.timeframe)

        # =======================================
        # 步骤 1: 向右看！补齐【最新】缺失的 K 线
        # =======================================
        time_diff_seconds = (current_local - last_local_time).total_seconds()
        missing_new_bars = int(time_diff_seconds / bar_seconds)

        new_df = pd.DataFrame()
        if missing_new_bars > 0:
            logger.debug(f"🔄 准备增量补齐约 {missing_new_bars} 根 最新 K 线...")
            new_df = self.fetch_from_okx(limit=missing_new_bars + 10)

        if not new_df.empty:
            combined_df = pd.concat([local_df, new_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index(ascending=True)
        else:
            combined_df = local_df

        # =======================================
        # 步骤 2: 向左看！补齐【更老】的历史 K 线
        # =======================================
        current_count = len(combined_df)
        old_df = pd.DataFrame()

        if current_count < limit:
            missing_old_bars = limit - current_count
            logger.debug(f"🔄 本地数据总量不足，准备向前追溯补齐 {missing_old_bars} 根 历史 K 线...")

            # 计算当前库中最老一根 K 线的时间，并逆向剥离时区还原为 UTC 毫秒时间戳
            oldest_local = combined_df.index[0]
            oldest_utc = oldest_local
            if "+" in TIMEZONE:
                oldest_utc -= pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
            elif "-" in TIMEZONE:
                oldest_utc += pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))

            oldest_ts_ms = str(int(oldest_utc.tz_localize('UTC').timestamp() * 1000))

            # 携带 after_ts 拉取更早的数据
            old_df = self.fetch_from_okx(limit=missing_old_bars + 10, after_ts=oldest_ts_ms)

        if not old_df.empty:
            combined_df = pd.concat([old_df, combined_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index(ascending=True)

        # =======================================
        # 步骤 3: 检测并补齐【中间】缺失的 K 线
        # =======================================
        missing_intervals = self._detect_missing_intervals(combined_df)
        if missing_intervals:
            logger.debug(f"🔍 检测到 {len(missing_intervals)} 个缺失区间，开始补全...")
            filled_dfs = []
            for i, (start_time, end_time) in enumerate(missing_intervals):
                logger.debug(f"  补全区间 {i + 1}: {start_time} -> {end_time}")
                filled_df = self._fill_missing_interval(start_time, end_time)
                if not filled_df.empty:
                    filled_dfs.append(filled_df)

            if filled_dfs:
                # 合并所有补全的数据
                filled_combined = pd.concat(filled_dfs)
                # 合并到主数据集
                combined_df = pd.concat([combined_df, filled_combined])
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df = combined_df.sort_index(ascending=True)
                logger.debug(f"✅ 成功补全 {len(filled_dfs)} 个缺失区间，新增 {len(filled_combined)} 根K线")

        # =======================================
        # 步骤 4: 保存至本地数据库并返回
        # =======================================
        # 将最终数据保存到本地数据库（增量追加）
        self._append_to_local_data(combined_df)

        return combined_df.tail(limit)

    def fetch_historical_data(self, limit=50000) -> pd.DataFrame:
        """
        全量智能拼接系统：
        分离了【增量拉取最新数据】和【追溯拉取历史数据】两个动作
        """
        return self._fetch_historical_data_with_limit(limit)

    def _get_seconds(self, timeframe: str) -> int:
        mapping = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,  # <--- 加上 1800 秒！
            '1H': 3600,
            '4H': 14400,
            '1D': 86400
        }
        if timeframe not in mapping:
            raise IndexError(f"没有这个timeframe: {timeframe}")

        return mapping.get(timeframe)

    def _calculate_bars_needed(self, start_date, end_date):
        """计算从 start_date 到 end_date 之间需要多少根 K 线"""
        if isinstance(start_date, str):
            start_date = pd.Timestamp(start_date)
        if isinstance(end_date, str):
            end_date = pd.Timestamp(end_date)

        bar_seconds = self._get_seconds(self.timeframe)
        total_seconds = (end_date - start_date).total_seconds()
        # 向上取整，确保覆盖整个时间段
        bars_needed = int(total_seconds // bar_seconds) + 1
        return max(bars_needed, 0)

    def _format_minute_time(self, value) -> str:
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")

    def _is_date_only_string(self, value) -> bool:
        if not isinstance(value, str):
            return False
        value = value.strip()
        return len(value) == 10 and value[4] == "-" and value[7] == "-"

    def _normalize_date_range(self, start_date, end_date):
        end_is_date_only = self._is_date_only_string(end_date)

        if isinstance(start_date, str):
            start_date = pd.Timestamp(start_date)
        if isinstance(end_date, str):
            end_date = pd.Timestamp(end_date)

        if end_is_date_only:
            end_date = end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=self._get_seconds(self.timeframe))

        return start_date, end_date

    def fetch_data_by_date_range(self, start_date, end_date):
        """
        智能且精准获取指定日期范围内的数据 (狙击版)
        绝不多拉一根不必要的 K 线
        """
        start_date, end_date = self._normalize_date_range(start_date, end_date)

        # 1. 首先加载本地数据
        local_df = self.load_local_data()

        if not local_df.empty:
            mask = (local_df.index >= start_date) & (local_df.index <= end_date)
            local_in_range = local_df[mask]

            if len(local_in_range) > 0:
                expected_bars = self._calculate_bars_needed(start_date, end_date)
                # 容错 5% 的缺失，如果够了直接返回
                if len(local_in_range) >= expected_bars * 0.95:
                    logger.debug(
                        f"✅ 本地数据库已覆盖 {self._format_minute_time(start_date)} 到 "
                        f"{self._format_minute_time(end_date)}，共 {len(local_in_range)} 根"
                    )
                    return local_in_range

        # 2. 本地数据不足，进行精准【定向拉取】
        bars_needed = self._calculate_bars_needed(start_date, end_date)
        if bars_needed == 0:
            return pd.DataFrame()

        buffer_bars = int(bars_needed * 1.05) + 10  # 只需要 5% 的极小缓冲
        logger.debug(
            f"🔄 准备【定向拉取】约 {buffer_bars} 根 K 线 "
            f"(目标区间: {self._format_minute_time(start_date)} -> {self._format_minute_time(end_date)})"
        )

        # 核心修复：把 end_date 转换为 OKX 认识的 UTC 毫秒时间戳，作为拉取起点！
        end_utc = end_date
        if "+" in TIMEZONE:
            end_utc -= pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
        elif "-" in TIMEZONE:
            end_utc += pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))

        # 加上 1000 毫秒的冗余，确保能拿到 end_date 那个周期本身的 K 线
        end_ts_ms = str(int(end_utc.timestamp() * 1000) + 1000)

        # 🎯 直接调用底层接口，强行从 end_date 往回拉！指哪打哪！
        fetched_df = self.fetch_from_okx(limit=buffer_bars, after_ts=end_ts_ms)

        if fetched_df.empty:
            logger.error("❌ 定向拉取数据失败，请检查网络或 OKX 接口状态。")
            return pd.DataFrame()

        # 3. 将新拉取的数据合并进本地数据库并持久化
        if not local_df.empty:
            combined_df = pd.concat([local_df, fetched_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index(ascending=True)
        else:
            combined_df = fetched_df

        self.save_local_data(combined_df)

        # 4. 再次切片返回
        mask = (combined_df.index >= start_date) & (combined_df.index <= end_date)
        result_df = combined_df[mask]

        if not result_df.empty:
            logger.debug(
                f"✅ 成功获取并合并 {self._format_minute_time(start_date)} 到 "
                f"{self._format_minute_time(end_date)} 的数据，共 {len(result_df)} 根 K 线"
            )

        return result_df

    def _detect_missing_intervals(self, df: pd.DataFrame) -> list:
        """
        检测时间序列中的缺失区间

        Args:
            df: 本地加载的K线数据，索引为timestamp

        Returns:
            list: 缺失区间列表，每个元素为(start_time, end_time)元组
        """
        if df.empty or len(df) < 2:
            return []

        # 确保按时间排序
        df = df.sort_index(ascending=True)

        # 计算理论时间间隔（秒）
        interval_seconds = self._get_seconds(self.timeframe)

        missing_intervals = []

        # 遍历相邻行，检测缺失
        for i in range(len(df) - 1):
            current_time = df.index[i]
            next_time = df.index[i + 1]

            # 计算实际时间差
            time_diff = (next_time - current_time).total_seconds()

            # 允许10%的容差（考虑数据可能略有偏差）
            if time_diff > interval_seconds * 1.1:
                # 计算缺失的条数
                missing_bars = int(time_diff // interval_seconds) - 1
                if missing_bars > 0:
                    # 计算缺失区间的开始和结束时间
                    gap_start = current_time + pd.Timedelta(seconds=interval_seconds)
                    gap_end = next_time - pd.Timedelta(seconds=interval_seconds)
                    missing_intervals.append((gap_start, gap_end))

        return missing_intervals

    def _fill_missing_interval(self, start_time, end_time) -> pd.DataFrame:
        """
        填充指定时间区间的缺失数据

        Args:
            start_time: 缺失区间开始时间
            end_time: 缺失区间结束时间

        Returns:
            pd.DataFrame: 填充的数据
        """
        # 计算需要多少根K线
        interval_seconds = self._get_seconds(self.timeframe)
        total_seconds = (end_time - start_time).total_seconds()
        bars_needed = int(total_seconds // interval_seconds) + 1

        logger.debug(f"🔄 准备填充缺失区间: {start_time} -> {end_time}, 约{bars_needed}根K线")

        # 将结束时间转换为OKX API需要的UTC毫秒时间戳
        end_utc = end_time
        if "+" in TIMEZONE:
            end_utc -= pd.Timedelta(hours=int(TIMEZONE.split("+")[-1]))
        elif "-" in TIMEZONE:
            end_utc += pd.Timedelta(hours=int(TIMEZONE.split("-")[-1]))

        # 加上1000毫秒冗余
        end_ts_ms = str(int(end_utc.timestamp() * 1000) + 1000)

        # 获取缺失数据
        missing_df = self.fetch_from_okx(limit=bars_needed + 10, after_ts=end_ts_ms)

        # 过滤出指定区间内的数据
        mask = (missing_df.index >= start_time) & (missing_df.index <= end_time)
        return missing_df[mask]

    def fetch_data_by_hours(self, hours: int) -> pd.DataFrame:
        """
        获取指定小时数的历史 K 线数据

        根据当前 timeframe 计算需要多少根 K 线，然后调用 fetch_historical_data 获取数据。
        例如：timeframe='1m', hours=24 -> 需要 24 * 60 = 1440 根 K 线

        Args:
            hours: 需要多少小时的数据

        Returns:
            pd.DataFrame: 包含 'open', 'high', 'low', 'close', 'volume' 列的 DataFrame
        """
        # 计算所需 K 线数量
        bar_seconds = self._get_seconds(self.timeframe)
        total_seconds = hours * 3600
        # 向上取整，确保覆盖整个时间段
        bars_needed = int(total_seconds // bar_seconds)

        logger.info(f"📡 正在获取 {self.symbol} {self.timeframe} 数据...")
        logger.info(f"   目标: 最近 {hours} 小时 (约 {bars_needed} 根 K 线)")

        # 调用现有方法获取数据
        df = self.fetch_historical_data(limit=bars_needed)

        if df.empty:
            logger.warning(f"❌ 获取数据失败：返回空DataFrame")
        else:
            logger.debug(f"✅ 成功获取 {len(df)} 根 K 线数据")
            logger.debug(f"   时间范围: {df.index[0]} 到 {df.index[-1]}")
            logger.debug(f"   价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
            logger.debug(f"   总成交量: {df['volume'].sum():.2f}")

        return df

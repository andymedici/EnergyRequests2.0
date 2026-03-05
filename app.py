"""
Power Interconnection Queue Monitor v3.1
=========================================
Tracks interconnection requests across all 7 US ISOs.
Database: PostgreSQL (via psycopg2 connection pool)

Sources:
- CAISO: gridstatus library
- NYISO: Direct Excel download
- ISO-NE: HTML table scraping
- SPP: Direct CSV download
- MISO: Free JSON API
- ERCOT: gridstatus library
- PJM: Berkeley Lab dataset (PJM's own API requires paid membership)

Scheduling: Weekly automated sync (configurable)
"""

APP_VERSION = "3.1.0"

import os
import sys
import json
import hashlib
import logging
import re
import time
import threading
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import psycopg2
import psycopg2.pool
import psycopg2.extras
import requests
import pandas as pd
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify, request, redirect, url_for, Response
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import gridstatus
    GRIDSTATUS_AVAILABLE = True
except ImportError:
    GRIDSTATUS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

DATA_DIR = os.environ.get('DATA_DIR', '/app/data')

# How often to run automatic syncs (in seconds). Default: weekly (604800s)
SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL_SECONDS', 604800))


# ---------------------------------------------------------------------------
# Database URL — Railway uses DATABASE_URL; fix postgres:// → postgresql://
# ---------------------------------------------------------------------------
def _get_database_url():
    url = os.environ.get('DATABASE_URL', '')
    if not url:
        logger.error("DATABASE_URL is not set. Add a PostgreSQL database in your Railway project.")
        sys.exit(1)
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    return url

DATABASE_URL = _get_database_url()


# ===========================================================================
# Database Layer — PostgreSQL with connection pooling
# ===========================================================================
class Database:
    """
    Thread-safe PostgreSQL wrapper using a connection pool.
    All queries use %s placeholders (psycopg2 standard).
    Rows are returned as RealDictRow (dict-like access by column name).
    """

    def __init__(self, dsn, min_conn=2, max_conn=10):
        self.pool = psycopg2.pool.ThreadedConnectionPool(min_conn, max_conn, dsn)
        self._init_schema()

    # -- connection context manager ----------------------------------------

    class _Conn:
        """Borrow a connection from the pool, auto-return on exit."""
        def __init__(self, pool):
            self.pool = pool
            self.conn = None
        def __enter__(self):
            self.conn = self.pool.getconn()
            return self.conn
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                self.conn.rollback()
            self.pool.putconn(self.conn)
            return False

    def _conn(self):
        return self._Conn(self.pool)

    # -- schema ------------------------------------------------------------

    def _init_schema(self):
        """
        Create tables if they don't exist.  Uses an advisory lock so that
        when gunicorn spawns multiple workers, only one actually runs the
        DDL — the others wait and then skip.  This avoids the race where
        two workers both try to CREATE TABLE with SERIAL columns and the
        second hits a UniqueViolation on the implicit sequence.
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                # Grab a session-level advisory lock (arbitrary key 1)
                cur.execute('SELECT pg_advisory_lock(1)')
                try:
                    # Check if tables already exist to skip DDL entirely
                    cur.execute("""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'projects'
                    """)
                    if cur.fetchone()[0] == 0:
                        cur.execute('''
                            CREATE TABLE projects (
                                id SERIAL PRIMARY KEY,
                                request_id TEXT UNIQUE NOT NULL,
                                project_name TEXT,
                                capacity_mw DOUBLE PRECISION,
                                county TEXT,
                                state TEXT,
                                customer TEXT,
                                utility TEXT,
                                status TEXT,
                                fuel_type TEXT,
                                source TEXT,
                                source_url TEXT,
                                project_type TEXT,
                                data_hash TEXT,
                                first_seen TIMESTAMP DEFAULT NOW(),
                                last_updated TIMESTAMP DEFAULT NOW()
                            );

                            CREATE TABLE monitor_runs (
                                id SERIAL PRIMARY KEY,
                                run_date TIMESTAMP DEFAULT NOW(),
                                status TEXT,
                                sources_checked INTEGER,
                                projects_found INTEGER,
                                projects_stored INTEGER,
                                duration_seconds DOUBLE PRECISION,
                                details TEXT
                            );

                            CREATE TABLE sync_log (
                                id SERIAL PRIMARY KEY,
                                source TEXT,
                                sync_time TIMESTAMP DEFAULT NOW(),
                                projects_found INTEGER,
                                projects_new INTEGER,
                                status TEXT,
                                error_message TEXT
                            );
                        ''')
                        for idx_sql in [
                            'CREATE INDEX IF NOT EXISTS idx_projects_utility ON projects(utility)',
                            'CREATE INDEX IF NOT EXISTS idx_projects_state ON projects(state)',
                            'CREATE INDEX IF NOT EXISTS idx_projects_type ON projects(project_type)',
                            'CREATE INDEX IF NOT EXISTS idx_projects_capacity ON projects(capacity_mw)',
                        ]:
                            cur.execute(idx_sql)
                        logger.info("PostgreSQL tables created")
                    else:
                        logger.info("PostgreSQL tables already exist")
                finally:
                    cur.execute('SELECT pg_advisory_unlock(1)')
            conn.commit()

    # -- query helpers -----------------------------------------------------

    def execute(self, query, params=()):
        """Execute a write query (INSERT / UPDATE / DELETE)."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()

    def fetchall(self, query, params=()):
        """Execute a read query, return list of RealDictRow."""
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def fetchone(self, query, params=()):
        """Execute a read query, return a single RealDictRow or None."""
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def bulk_upsert_projects(self, projects):
        """
        Upsert a batch of projects in a single transaction using
        INSERT ... ON CONFLICT. Returns count of new inserts.
        """
        new_count = 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                for p in projects:
                    try:
                        cur.execute('''
                            INSERT INTO projects
                                (request_id, project_name, capacity_mw, county, state,
                                 customer, utility, status, fuel_type, source,
                                 source_url, project_type, data_hash)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (request_id) DO UPDATE SET
                                project_name  = EXCLUDED.project_name,
                                capacity_mw   = EXCLUDED.capacity_mw,
                                county        = EXCLUDED.county,
                                state         = EXCLUDED.state,
                                customer      = EXCLUDED.customer,
                                utility       = EXCLUDED.utility,
                                status        = EXCLUDED.status,
                                fuel_type     = EXCLUDED.fuel_type,
                                source        = EXCLUDED.source,
                                source_url    = EXCLUDED.source_url,
                                project_type  = EXCLUDED.project_type,
                                data_hash     = EXCLUDED.data_hash,
                                last_updated  = NOW()
                            RETURNING (xmax = 0) AS inserted
                        ''', (
                            p['request_id'], p['project_name'], p['capacity_mw'],
                            p.get('county', ''), p.get('state', ''), p.get('customer', ''),
                            p['utility'], p.get('status', ''), p.get('fuel_type', ''),
                            p['source'], p.get('source_url', ''), p.get('project_type', ''),
                            p['data_hash'],
                        ))
                        row = cur.fetchone()
                        if row and row[0]:
                            new_count += 1
                    except Exception as e:
                        logger.debug(f"Upsert failed for {p.get('request_id','?')}: {e}")
                        conn.rollback()
            conn.commit()
        return new_count


db = Database(DATABASE_URL)


# ===========================================================================
# Data Fetcher  (unchanged logic, same 7 ISOs)
# ===========================================================================
class PowerQueueFetcher:
    """Fetches interconnection queue data from all 7 US ISOs."""

    def __init__(self, min_capacity_mw=75):
        self.min_capacity_mw = min_capacity_mw
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self._berkeley_cache = {}

    # -- helpers ----------------------------------------------------------

    def _parse_capacity(self, value):
        if pd.isna(value) or value is None or value == '':
            return None
        text = str(value).replace(',', '').strip()
        for suffix in ['MW', 'mw', 'Mw', 'MEGAWATT']:
            text = text.replace(suffix, '')
        text = text.strip()
        try:
            cap = float(text)
            return cap if cap >= self.min_capacity_mw else None
        except ValueError:
            m = re.search(r'(\d+\.?\d*)', text)
            if m:
                try:
                    cap = float(m.group(1))
                    return cap if cap >= self.min_capacity_mw else None
                except ValueError:
                    pass
        return None

    @staticmethod
    def _hash(data):
        key = f"{data.get('project_name','')}_{data.get('capacity_mw',0)}_{data.get('state','')}_{data.get('utility','')}"
        return hashlib.md5(key.lower().encode()).hexdigest()

    @staticmethod
    def _classify(name, customer='', fuel_type=''):
        text = f"{name} {customer} {fuel_type}".lower()
        if any(k in text for k in ['data center', 'datacenter', 'cloud', 'hyperscale',
                                    'colocation', 'microsoft', 'amazon', 'google',
                                    'meta', 'aws', 'facebook']):
            return 'datacenter'
        if any(k in text for k in ['battery', 'storage', 'bess', 'energy storage']):
            return 'storage'
        if any(k in text for k in ['solar', 'photovoltaic', 'pv ']):
            return 'solar'
        if any(k in text for k in ['wind', 'offshore']):
            return 'wind'
        if any(k in text for k in ['natural gas', 'gas turbine', 'combined cycle',
                                    'peaker', 'ccgt']):
            return 'gas'
        if 'nuclear' in text:
            return 'nuclear'
        return 'other'

    def _build(self, **kw):
        kw.setdefault('project_type', self._classify(
            kw.get('project_name', ''),
            kw.get('customer', ''),
            kw.get('fuel_type', '')
        ))
        kw['data_hash'] = self._hash(kw)
        return kw

    # -- CAISO ------------------------------------------------------------

    def fetch_caiso(self):
        projects = []
        if not GRIDSTATUS_AVAILABLE:
            logger.warning("CAISO: gridstatus not available, skipping")
            return projects
        try:
            logger.info("CAISO: Fetching via gridstatus")
            caiso = gridstatus.CAISO()
            df = caiso.get_interconnection_queue()
            logger.info(f"CAISO: {len(df)} rows")
            for _, r in df.iterrows():
                cap = self._parse_capacity(r.get('Capacity (MW)', 0))
                if cap:
                    projects.append(self._build(
                        request_id=f"CAISO_{r.get('Queue ID', r.name)}",
                        project_name=str(r.get('Project Name', 'Unknown'))[:500],
                        capacity_mw=cap,
                        county=str(r.get('County', ''))[:200],
                        state='CA',
                        customer=str(r.get('Interconnection Customer', ''))[:500],
                        utility='CAISO',
                        status=str(r.get('Status', 'Active')),
                        fuel_type=str(r.get('Fuel', '')),
                        source='CAISO', source_url='gridstatus',
                    ))
            logger.info(f"CAISO: {len(projects)} projects extracted")
        except Exception as e:
            logger.error(f"CAISO failed: {e}")
        return projects

    # -- NYISO ------------------------------------------------------------

    def fetch_nyiso(self):
        projects = []
        url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        try:
            logger.info("NYISO: Downloading queue spreadsheet")
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.error(f"NYISO: HTTP {resp.status_code}")
                return projects
            df = pd.read_excel(BytesIO(resp.content))
            logger.info(f"NYISO: {len(df)} rows")
            mw_cols = [c for c in df.columns if 'MW' in str(c).upper()]
            for _, r in df.iterrows():
                cap = None
                for col in mw_cols:
                    cap = self._parse_capacity(r.get(col))
                    if cap:
                        break
                if cap:
                    projects.append(self._build(
                        request_id=f"NYISO_{r.get('Queue Position', r.name)}",
                        project_name=str(r.get('Project Name', r.get('Proposed Name', 'Unknown')))[:500],
                        capacity_mw=cap,
                        county=str(r.get('County', ''))[:200],
                        state='NY',
                        customer=str(r.get('Developer', ''))[:500],
                        utility='NYISO',
                        status=str(r.get('Status', 'Active')),
                        fuel_type=str(r.get('Type', '')),
                        source='NYISO', source_url=url,
                    ))
            logger.info(f"NYISO: {len(projects)} projects extracted")
        except Exception as e:
            logger.error(f"NYISO failed: {e}")
        return projects

    # -- ISO-NE -----------------------------------------------------------

    def fetch_isone(self):
        projects = []
        url = 'https://irtt.iso-ne.com/reports/external'
        try:
            logger.info("ISO-NE: Scraping queue table")
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.error(f"ISO-NE: HTTP {resp.status_code}")
                return projects
            soup = BeautifulSoup(resp.content, 'html.parser')
            table = soup.find('table')
            if not table:
                logger.warning("ISO-NE: No table found")
                return projects
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if len(cells) < len(headers):
                    continue
                rd = {headers[i]: cells[i].get_text(strip=True) for i in range(len(headers))}
                cap = None
                for mw_col in ['Net MW', 'Summer MW', 'Winter MW', 'MW']:
                    if mw_col in rd:
                        cap = self._parse_capacity(rd[mw_col])
                        if cap:
                            break
                if cap:
                    projects.append(self._build(
                        request_id=f"ISONE_{rd.get('QP', len(projects))}",
                        project_name=str(rd.get('Alternative Name', rd.get('Unit', 'Unknown')))[:500],
                        capacity_mw=cap,
                        county=str(rd.get('County', ''))[:200],
                        state=str(rd.get('ST', 'MA'))[:2],
                        customer='',
                        utility='ISO-NE',
                        status=str(rd.get('Status', 'Active')),
                        fuel_type=str(rd.get('Fuel Type', '')),
                        source='ISO-NE', source_url=url,
                    ))
            logger.info(f"ISO-NE: {len(projects)} projects extracted")
        except Exception as e:
            logger.error(f"ISO-NE failed: {e}")
        return projects

    # -- SPP --------------------------------------------------------------

    def fetch_spp(self):
        projects = []
        url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        try:
            logger.info("SPP: Downloading CSV")
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.error(f"SPP: HTTP {resp.status_code}")
                return projects
            lines = resp.text.split('\n')
            header_idx = 0
            for i, line in enumerate(lines[:10]):
                if 'MW' in line or 'Generation' in line:
                    header_idx = i
                    break
            df = pd.read_csv(StringIO('\n'.join(lines[header_idx:])))
            logger.info(f"SPP: {len(df)} rows")
            mw_cols = [c for c in df.columns if 'MW' in str(c).upper()]
            for _, r in df.iterrows():
                cap = None
                for col in mw_cols:
                    cap = self._parse_capacity(r.get(col))
                    if cap:
                        break
                if cap:
                    projects.append(self._build(
                        request_id=f"SPP_{r.get('Generation Interconnection Number', r.name)}",
                        project_name=str(r.get('Project Name', 'Unknown'))[:500],
                        capacity_mw=cap,
                        county=str(r.get(' Nearest Town or County', ''))[:200],
                        state=str(r.get('State', ''))[:2],
                        customer='',
                        utility='SPP',
                        status=str(r.get('Status', 'Active')),
                        fuel_type=str(r.get('Fuel Type', r.get('Generation Type', ''))),
                        source='SPP', source_url=url,
                    ))
            logger.info(f"SPP: {len(projects)} projects extracted")
        except Exception as e:
            logger.error(f"SPP failed: {e}")
        return projects

    # -- MISO -------------------------------------------------------------

    def fetch_miso(self):
        projects = self._fetch_miso_api()
        if not projects and GRIDSTATUS_AVAILABLE:
            logger.info("MISO: API failed, trying gridstatus")
            projects = self._fetch_miso_gridstatus()
        return projects

    def _fetch_miso_api(self):
        projects = []
        url = "https://www.misoenergy.org/api/giqueue/getprojects"
        try:
            logger.info("MISO: JSON API")
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                return projects
            data = resp.json()
            if not data:
                return projects
            logger.info(f"MISO: {len(data)} rows")
            for item in data:
                cap = None
                for f in ['summerNetMW', 'winterNetMW', 'mw', 'MW', 'capacity', 'netMW']:
                    v = item.get(f)
                    if v is not None:
                        cap = self._parse_capacity(v)
                        if cap:
                            break
                if cap:
                    projects.append(self._build(
                        request_id=f"MISO_{item.get('jNumber', item.get('queueNumber', 'UNK'))}",
                        project_name=str(item.get('projectName', item.get('name', 'Unknown')))[:500],
                        capacity_mw=cap,
                        county=str(item.get('county', ''))[:200],
                        state=str(item.get('state', ''))[:2],
                        customer=str(item.get('interconnectionEntity', item.get('developer', '')))[:500],
                        utility='MISO',
                        status=str(item.get('status', item.get('queueStatus', 'Active'))),
                        fuel_type=str(item.get('fuelType', item.get('fuel', ''))),
                        source='MISO', source_url=url,
                    ))
            logger.info(f"MISO: {len(projects)} projects")
        except Exception as e:
            logger.error(f"MISO API failed: {e}")
        return projects

    def _fetch_miso_gridstatus(self):
        projects = []
        try:
            miso = gridstatus.MISO()
            df = miso.get_interconnection_queue()
            for _, r in df.iterrows():
                cap = self._parse_capacity(r.get('Capacity (MW)') or r.get('summerNetMW') or 0)
                if cap:
                    projects.append(self._build(
                        request_id=f"MISO_{r.get('Queue ID', r.get('jNumber', r.name))}",
                        project_name=str(r.get('Project Name', 'Unknown'))[:500],
                        capacity_mw=cap,
                        county=str(r.get('County', r.get('county', '')))[:200],
                        state=str(r.get('State', r.get('state', '')))[:2],
                        customer=str(r.get('Interconnecting Entity', ''))[:500],
                        utility='MISO',
                        status=str(r.get('Status', 'Active')),
                        fuel_type=str(r.get('Fuel Type', '')),
                        source='MISO', source_url='gridstatus',
                    ))
        except Exception as e:
            logger.error(f"MISO gridstatus failed: {e}")
        return projects

    # -- ERCOT ------------------------------------------------------------

    def fetch_ercot(self):
        projects = []
        if not GRIDSTATUS_AVAILABLE:
            logger.warning("ERCOT: gridstatus not available")
            return projects
        try:
            logger.info("ERCOT: Fetching via gridstatus")
            ercot = gridstatus.Ercot()
            df = ercot.get_interconnection_queue()
            logger.info(f"ERCOT: {len(df)} rows")
            for _, r in df.iterrows():
                cap = self._parse_capacity(r.get('Capacity (MW)') or r.get('Summer MW') or 0)
                if cap:
                    projects.append(self._build(
                        request_id=f"ERCOT_{r.get('Queue ID', r.name)}",
                        project_name=str(r.get('Project Name', 'Unknown'))[:500],
                        capacity_mw=cap,
                        county=str(r.get('County', ''))[:200],
                        state='TX',
                        customer=str(r.get('Interconnecting Entity', ''))[:500],
                        utility='ERCOT',
                        status=str(r.get('Status', 'Active')),
                        fuel_type=str(r.get('Fuel', r.get('Technology', ''))),
                        source='ERCOT', source_url='gridstatus',
                    ))
            logger.info(f"ERCOT: {len(projects)} projects")
        except Exception as e:
            logger.error(f"ERCOT failed: {e}")
        return projects

    # -- PJM (Berkeley Lab) -----------------------------------------------

    def fetch_pjm(self):
        if 'PJM' in self._berkeley_cache:
            return self._berkeley_cache['PJM']
        logger.info("PJM: Fetching from Berkeley Lab")
        all_bl = self._fetch_berkeley_lab()
        pjm = [p for p in all_bl if p.get('utility') == 'PJM']
        logger.info(f"PJM: {len(pjm)} projects")
        return pjm

    # -- Berkeley Lab -----------------------------------------------------

    def _fetch_berkeley_lab(self):
        projects = []
        urls = [
            'https://emp.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            'https://emp.lbl.gov/sites/default/files/2025-12/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            'https://eta-publications.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            'https://emp.lbl.gov/sites/default/files/2024-04/queued_up_2024_data_file.xlsx',
        ]
        header_sets = [
            {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
             'Referer': 'https://emp.lbl.gov/queues',
             'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*'},
            {'User-Agent': 'curl/7.88.1', 'Accept': '*/*'},
        ]

        df = None
        excel_content = None
        selected_sheet = None

        for url in urls:
            if df is not None:
                break
            for headers in header_sets:
                try:
                    logger.info(f"Berkeley Lab: Trying {url}")
                    resp = self.session.get(url, headers=headers, timeout=120, allow_redirects=True)
                    if resp.status_code != 200 or len(resp.content) < 100_000:
                        continue
                    if resp.content[:2] != b'PK':
                        continue
                    excel_file = pd.ExcelFile(BytesIO(resp.content))
                    logger.info(f"Berkeley Lab: Sheets: {excel_file.sheet_names}")
                    data_sheet = self._find_data_sheet(excel_file)
                    if data_sheet:
                        df = pd.read_excel(excel_file, sheet_name=data_sheet)
                        excel_content = resp.content
                        selected_sheet = data_sheet
                        logger.info(f"Berkeley Lab: '{data_sheet}' → {len(df)} rows")
                        break
                except Exception as e:
                    logger.debug(f"Berkeley Lab: Failed {url}: {e}")

        if df is None:
            for path in [os.path.join(DATA_DIR, f) for f in ['queued_up_data.xlsx', 'berkeley_lab.xlsx']]:
                if os.path.exists(path):
                    try:
                        df = pd.read_excel(path, sheet_name=0)
                        logger.info(f"Berkeley Lab: cache {path}")
                        break
                    except Exception:
                        pass

        if df is None:
            logger.error("Berkeley Lab: All attempts failed")
            return projects

        df = self._fix_header_row(df, excel_content, selected_sheet)
        df.columns = [str(c).strip() for c in df.columns]

        def find_col(names):
            for n in names:
                for c in df.columns:
                    if n.lower() in str(c).lower():
                        return c
            return None

        entity_col = find_col(['entity', 'region', 'iso', 'rto', 'ba'])
        mw_col     = find_col(['capacity_mw', 'mw', 'capacity', 'nameplate'])
        name_col   = find_col(['project_name', 'project', 'name'])
        id_col     = find_col(['queue_id', 'request_id', 'queue_pos', 'position'])
        state_col  = find_col(['state'])
        county_col = find_col(['county'])
        status_col = find_col(['queue_status', 'status'])
        fuel_col   = find_col(['resource_type', 'resource', 'fuel', 'type', 'technology'])
        dev_col    = find_col(['developer', 'interconnection', 'owner', 'applicant'])

        ENTITY_MAP = {
            'PJM': 'PJM', 'MISO': 'MISO', 'CAISO': 'CAISO', 'CALIFORNIA': 'CAISO',
            'ERCOT': 'ERCOT', 'TEXAS': 'ERCOT', 'SPP': 'SPP',
            'NYISO': 'NYISO', 'NEW YORK': 'NYISO',
            'ISO-NE': 'ISO-NE', 'ISONE': 'ISO-NE', 'NEW ENGLAND': 'ISO-NE',
        }

        for idx, row in df.iterrows():
            try:
                raw = str(row.get(entity_col, '') if entity_col else '').upper()
                utility = 'Other'
                for key, val in ENTITY_MAP.items():
                    if key in raw:
                        utility = val
                        break
                cap = self._parse_capacity(row.get(mw_col, 0) if mw_col else 0)
                if not cap:
                    continue
                projects.append(self._build(
                    request_id=f"{utility}_BL_{row.get(id_col, idx) if id_col else idx}",
                    project_name=str(row.get(name_col, 'Unknown') if name_col else 'Unknown')[:500],
                    capacity_mw=cap,
                    county=str(row.get(county_col, '') if county_col else '')[:200],
                    state=str(row.get(state_col, '') if state_col else '')[:2],
                    customer=str(row.get(dev_col, '') if dev_col else '')[:500],
                    utility=utility,
                    status=str(row.get(status_col, 'Active') if status_col else 'Active'),
                    fuel_type=str(row.get(fuel_col, '') if fuel_col else ''),
                    source=f'{utility} (Berkeley Lab)',
                    source_url='emp.lbl.gov',
                ))
            except Exception:
                continue

        self._berkeley_cache.clear()
        for p in projects:
            self._berkeley_cache.setdefault(p['utility'], []).append(p)

        breakdown = {}
        for p in projects:
            breakdown[p['utility']] = breakdown.get(p['utility'], 0) + 1
        logger.info(f"Berkeley Lab: {len(projects)} total — {breakdown}")
        return projects

    def _find_data_sheet(self, excel_file):
        for sheet in excel_file.sheet_names:
            sl = sheet.lower()
            if 'complete' in sl and ('data' in sl or 'queue' in sl):
                return sheet
        for sheet in excel_file.sheet_names:
            sl = sheet.lower()
            if 'full' in sl and 'data' in sl:
                return sheet
        for sheet in excel_file.sheet_names:
            sl = sheet.lower()
            if 'all' in sl and any(k in sl for k in ['request', 'project', 'queue']):
                return sheet
        skip = {'sample', 'summary', 'background', 'method', 'intro', 'content', 'codebook'}
        for sheet in excel_file.sheet_names:
            sl = sheet.lower()
            if any(s in sl for s in skip):
                continue
            if any(k in sl for k in ['data', 'queue', 'project', 'active', 'request']):
                return sheet
        return excel_file.sheet_names[1] if len(excel_file.sheet_names) > 1 else excel_file.sheet_names[0]

    def _fix_header_row(self, df, excel_content, sheet_name):
        keywords = ['entity', 'region', 'queue', 'capacity', 'mw', 'state',
                     'county', 'status', 'resource', 'developer']
        for idx in range(min(20, len(df))):
            vals = [str(v).lower() for v in df.iloc[idx].values if pd.notna(v)]
            if sum(1 for k in keywords if k in ' '.join(vals)) >= 3:
                if excel_content and sheet_name:
                    ef = pd.ExcelFile(BytesIO(excel_content))
                    return pd.read_excel(ef, sheet_name=sheet_name, header=idx + 1)
                break
        return df

    # -- orchestrator -----------------------------------------------------

    def run_full_sync(self):
        start = time.time()
        sources = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('ISO-NE', self.fetch_isone),
            ('SPP', self.fetch_spp),
            ('MISO', self.fetch_miso),
            ('ERCOT', self.fetch_ercot),
            ('PJM', self.fetch_pjm),
        ]

        all_projects = []
        stats = {}

        for name, fn in sources:
            try:
                logger.info(f"--- Fetching {name} ---")
                results = fn()
                all_projects.extend(results)
                stats[name] = len(results)
                db.execute(
                    'INSERT INTO sync_log (source, projects_found, projects_new, status) VALUES (%s,%s,0,%s)',
                    (name, len(results), 'success')
                )
            except Exception as e:
                logger.error(f"{name} failed: {e}")
                stats[name] = 0
                db.execute(
                    'INSERT INTO sync_log (source, projects_found, projects_new, status, error_message) VALUES (%s,0,0,%s,%s)',
                    (name, 'error', str(e))
                )

        new_count = db.bulk_upsert_projects(all_projects)
        duration = time.time() - start

        db.execute('''
            INSERT INTO monitor_runs (status, sources_checked, projects_found, projects_stored, duration_seconds, details)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', ('success', len(sources), len(all_projects), new_count, duration, json.dumps(stats)))

        logger.info(f"Sync complete: {len(all_projects)} found, {new_count} new, {duration:.1f}s")
        return {
            'sources_checked': len(sources),
            'projects_found': len(all_projects),
            'projects_stored': new_count,
            'duration_seconds': round(duration, 1),
            'by_source': stats,
        }


fetcher = PowerQueueFetcher(min_capacity_mw=75)


# ===========================================================================
# Background Scheduler
# ===========================================================================
_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    # Only one gunicorn worker should run the scheduler.
    # Use a non-blocking try lock (key 3).  If we don't get it,
    # another worker already owns the scheduler — skip.
    try:
        got_lock = db.fetchone('SELECT pg_try_advisory_lock(3) AS locked')['locked']
        if not got_lock:
            logger.info("Scheduler owned by another worker, skipping")
            return
    except Exception:
        return  # If the lock check fails, don't start a scheduler

    def loop():
        while True:
            time.sleep(SYNC_INTERVAL)
            logger.info("Scheduled sync starting...")
            try:
                fetcher.run_full_sync()
            except Exception as e:
                logger.error(f"Scheduled sync failed: {e}")

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    logger.info(f"Scheduler started (every {SYNC_INTERVAL}s / {SYNC_INTERVAL/3600:.0f}h)")


# ===========================================================================
# Flask Routes — Pages
# ===========================================================================

@app.route('/')
def index():
    total = db.fetchone('SELECT COUNT(*) AS count FROM projects')['count']
    total_mw = db.fetchone('SELECT COALESCE(SUM(capacity_mw),0) AS total FROM projects')['total']
    by_utility = db.fetchall(
        'SELECT utility, COUNT(*) AS count, SUM(capacity_mw) AS total_mw FROM projects GROUP BY utility ORDER BY count DESC'
    )
    by_type = db.fetchall(
        'SELECT project_type, COUNT(*) AS count FROM projects GROUP BY project_type ORDER BY count DESC'
    )
    recent = db.fetchall('SELECT * FROM projects ORDER BY first_seen DESC LIMIT 10')
    last_run = db.fetchone('SELECT * FROM monitor_runs ORDER BY run_date DESC LIMIT 1')

    return render_template('index.html',
        total=total, total_mw=total_mw, by_utility=by_utility, by_type=by_type,
        recent=recent, last_run=last_run, gridstatus_available=GRIDSTATUS_AVAILABLE,
        version=APP_VERSION,
    )


@app.route('/projects')
def projects_page():
    return render_template('projects.html')


@app.route('/project/<int:id>')
def project_detail(id):
    project = db.fetchone('SELECT * FROM projects WHERE id = %s', (id,))
    if not project:
        return redirect(url_for('projects_page'))
    return render_template('project_detail.html', project=project)


@app.route('/monitoring')
def monitoring():
    runs = db.fetchall('SELECT * FROM monitor_runs ORDER BY run_date DESC LIMIT 20')
    source_stats = db.fetchall(
        'SELECT source, MAX(sync_time) AS last_sync, SUM(projects_found) AS total_found FROM sync_log GROUP BY source'
    )
    return render_template('monitoring.html', runs=runs, source_stats=source_stats,
                           sync_interval=SYNC_INTERVAL)


@app.route('/trigger')
def trigger_sync():
    result = fetcher.run_full_sync()
    return render_template('scan_results.html', result=result)


@app.route('/export')
def export_csv():
    rows = db.fetchall('''
        SELECT request_id, project_name, capacity_mw, county, state, customer,
               utility, status, fuel_type, project_type, source, first_seen
        FROM projects ORDER BY capacity_mw DESC
    ''')
    header = 'Request ID,Project Name,Capacity MW,County,State,Customer,Utility,Status,Fuel Type,Type,Source,First Seen'
    lines = [header]
    for r in rows:
        fields = [
            f'"{r["request_id"]}"',
            f'"{(r["project_name"] or "").replace(chr(34), chr(39))}"',
            str(r['capacity_mw']),
            f'"{r["county"] or ""}"',
            f'"{r["state"] or ""}"',
            f'"{(r["customer"] or "").replace(chr(34), chr(39))}"',
            f'"{r["utility"]}"',
            f'"{r["status"] or ""}"',
            f'"{r["fuel_type"] or ""}"',
            f'"{r["project_type"] or ""}"',
            f'"{r["source"] or ""}"',
            f'"{r["first_seen"]}"',
        ]
        lines.append(','.join(fields))
    return Response('\n'.join(lines), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=power_projects_{datetime.now():%Y%m%d}.csv'})


# ===========================================================================
# Flask Routes — JSON API
# ===========================================================================

@app.route('/api/projects')
def api_projects():
    rows = db.fetchall('''
        SELECT id, request_id, project_name, capacity_mw, county, state,
               customer, utility, status, fuel_type, project_type, source, first_seen
        FROM projects ORDER BY capacity_mw DESC
    ''')
    result = []
    for r in rows:
        d = dict(r)
        # Serialize datetime for JSON
        if d.get('first_seen'):
            d['first_seen'] = str(d['first_seen'])
        result.append(d)
    return jsonify(result)


@app.route('/api/stats')
def api_stats():
    total = db.fetchone('SELECT COUNT(*) AS c FROM projects')['c']
    by_utility = [dict(r) for r in db.fetchall(
        'SELECT utility, COUNT(*) AS count, SUM(capacity_mw) AS total_mw FROM projects GROUP BY utility'
    )]
    by_state = [dict(r) for r in db.fetchall(
        "SELECT state, COUNT(*) AS count FROM projects WHERE state != '' GROUP BY state ORDER BY count DESC"
    )]
    return jsonify({'total_projects': total, 'by_utility': by_utility, 'by_state': by_state})


@app.route('/api/sync', methods=['POST'])
def api_sync():
    return jsonify(fetcher.run_full_sync())


# ===========================================================================
# Startup
# ===========================================================================

def init_app():
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Power Monitor v{APP_VERSION} | PostgreSQL | gridstatus: {GRIDSTATUS_AVAILABLE}")

    # Use an advisory lock so only one gunicorn worker runs the initial sync.
    # pg_try_advisory_lock returns true if we got the lock, false if another
    # worker already holds it.  Lock key 2 (key 1 is used by schema init).
    try:
        got_lock = db.fetchone('SELECT pg_try_advisory_lock(2) AS locked')['locked']
        if got_lock:
            try:
                count = db.fetchone('SELECT COUNT(*) AS c FROM projects')['c']
                if count == 0:
                    logger.info("Empty database — running initial sync")
                    fetcher.run_full_sync()
                else:
                    logger.info(f"Database has {count} projects, skipping initial sync")
            except Exception as e:
                logger.error(f"Initial sync failed: {e}")
            finally:
                db.execute('SELECT pg_advisory_unlock(2)')
        else:
            logger.info("Another worker is handling initial sync, skipping")
    except Exception as e:
        logger.error(f"init_app error: {e}")

    start_scheduler()

    start_scheduler()


init_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

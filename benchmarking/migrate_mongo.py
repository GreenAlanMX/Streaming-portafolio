import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne


def load_env():
    env_local = Path('.env')
    env_example = Path('benchmarking/.env.example')
    if env_local.exists():
        load_dotenv(env_local)
    elif env_example.exists():
        load_dotenv(env_example)


@dataclass
class MongoConfig:
    host: str
    port: int
    db: str
    collection: str


def get_mongo_config() -> MongoConfig:
    load_env()
    return MongoConfig(
        host=os.getenv('MONGO_HOST', 'localhost'),
        port=int(os.getenv('MONGO_PORT', '27017')),
        db=os.getenv('MONGO_DB', 'benchmark_db'),
        collection=os.getenv('MONGO_COLLECTION', 'test_collection'),
    )


def get_collection(cfg: MongoConfig):
    client = MongoClient(cfg.host, cfg.port)
    return client[cfg.db][cfg.collection], client


def init_collection():
    cfg = get_mongo_config()
    col, client = get_collection(cfg)
    try:
        col.create_index('id', unique=True)
        print('MongoDB collection initialized with index on id.')
    finally:
        client.close()


def migrate_json(json_path: Path, batch_size: int = 1000) -> int:
    cfg = get_mongo_config()
    col, client = get_collection(cfg)
    count = 0
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        ops = []
        for doc in data:
            ops.append(InsertOne(doc))
            if len(ops) >= batch_size:
                col.bulk_write(ops, ordered=False)
                count += len(ops)
                ops.clear()
        if ops:
            col.bulk_write(ops, ordered=False)
            count += len(ops)
        return count
    finally:
        client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--init', action='store_true')
    parser.add_argument('--json', type=str, help='Path to JSON file')
    parser.add_argument('--batch', type=int, default=int(os.getenv('BATCH_SIZE', '1000')))
    args = parser.parse_args()

    if args.init:
        init_collection()
    elif args.json:
        total = migrate_json(Path(args.json), batch_size=args.batch)
        print(f'Migrated {total} documents to MongoDB')
    else:
        parser.print_help()

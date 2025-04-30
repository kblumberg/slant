import requests
from solana.rpc.api import Client
from constants.keys import SOLANA_RPC_URL
from solana.transaction import Signature


def parse_tx(tx_id: str):
    client = Client(SOLANA_RPC_URL)
    sig = Signature.from_string(tx_id)
    tx_data = client.get_transaction(sig, encoding="jsonParsed", max_supported_transaction_version=0)
    return tx_data
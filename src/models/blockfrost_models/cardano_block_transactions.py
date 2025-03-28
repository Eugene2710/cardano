from pydantic import BaseModel


class CardanoBlockTransactions(BaseModel):
    """
    this pydantic class is referring to the fetching of transactions given the block number/hash
    source: https://docs.blockfrost.io/#tag/cardano--blocks/GET/blocks/{hash_or_number}/txs
    sample output from the block transactions API:
        [
      "8788591983aa73981fc92d6cddbbe643959f5a784e84b8bee0db15823f575a5b",
      "4eef6bb7755d8afbeac526b799f3e32a624691d166657e9d862aaeb66682c036",
      "52e748c4dec58b687b90b0b40d383b9fe1f24c1a833b7395cdf07dd67859f46f",
      "e8073fd5318ff43eca18a852527166aa8008bee9ee9e891f585612b7e4ba700b"
    ]
    """

    tx_hash: list[str]

    @staticmethod
    def from_json(input: list[str]) -> "CardanoBlockTransactions":
        return CardanoBlockTransactions(tx_hash=input)

import pytest
from src.models.blockfrost_models.cardano_transaction_utxo import (
    TransactionUTxO,
    TransactionInput,
    TransactionOutput,
    Amount,
)
from src.models.database_transfer_objects.cardano_transactions_utxo_dto import (
    CardanoTransactionUtxoDTO,
)
from datetime import datetime
from decimal import Decimal


class TestCardanoTxUtxoDTO:
    """
    test if a single TransactionUTxO, a service level data class, is converted into a CardanoTransactionUtxoDTO using the
    from_raw_cardano_tx_utxo method
    Prepare: dummy TransactionUTxO object
    Act: use from_raw_cardano_tx_utxo
    Assert: check if the TransactionUTxO data are of the right data type
    Teardown: None
    """

    @pytest.fixture()
    def dummy_transaction_utxo(self) -> TransactionUTxO:

        return TransactionUTxO(
            hash="e68aee4ca7d0993c1f06eb3024d53b1fb34ca79663814bd9b86666a1d0ec8d7f",
            inputs=[
                TransactionInput(
                    address="addr1q93l79hdpvaeqnnmdkshmr4mpjvxnacqxs967keht465tt2dn0z9uhgereqgjsw33ka6c8tu5um7hqsnf5fd50fge9gq4lu2ql",
                    amount=[
                        Amount(unit="lovelace", quantity="2000000"),
                        Amount(
                            unit="29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494e",
                            quantity="50000000000",
                        ),
                    ],
                    tx_hash="1a689447f5d8b770a7da5c8b6a7ca224ae83dae3ed746d839dc379eeaef06f14",
                    output_index=1,
                    data_hash=None,
                    inline_datum=None,
                    reference_script_hash=None,
                    collateral=False,
                )
            ],
            outputs=[
                TransactionOutput(
                    address="addr1w8p79rpkcdz8x9d6tft0x0dx5mwuzac2sa4gm8cvkw5hcnqst2ctf",
                    amount=[Amount(unit="lovelace", quantity="682590846")],
                    output_index=0,
                    data_hash="8829fad87e1064de529788f5f2ac69604096e43cb8223aaf90edbbab91662408",
                    inline_datum="d8799fd8799f581c63ff16ed0b3b904e7b6da17d8ebb0c9869f700340baf5b375d7545adffd8799fd8799f581c63ff16ed0b3b904e7b6da17d8ebb0c9869f700340baf5b375d7545adffd8799fd8799fd8799f581c4d9bc45e5d191e408941d18dbbac1d7ca737eb82134d12da3d28c950ffffffffd87980d8799fd8799f581c63ff16ed0b3b904e7b6da17d8ebb0c9869f700340baf5b375d7545adffd8799fd8799fd8799f581c4d9bc45e5d191e408941d18dbbac1d7ca737eb82134d12da3d28c950ffffffffd87980d8799f581cf5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c58202ffadbb87144e875749122e0bbb9f535eeaa7f5660c6c4a91bcc4121e477f08dffd8799fd87a80d8799f1a287a1a9eff1a00019b29d87980ff1a0016e360d87a80ff",
                    collateral=False,
                    reference_script_hash=None,
                    consumed_by_tx="df4cfde288ad2d6f250da775404da97a0f43167fd858b1731d865bd7ab160bf5",
                )
            ],
        )

    def test_from_raw_cardano_tx_utxo(
        self, dummy_transaction_utxo: TransactionUTxO
    ) -> None:
        """
        GIVEN a TransactionUTXO service level data class
        WHEN CardanoTransactionUtxoDTO.from_raw_cardano_tx_utxo is invoked
        THEN the resulting DTO matches the original data except for generated IDs, timestamps and additonal hashes that were repeatedly appended into input and output
        """
        dto: CardanoTransactionUtxoDTO = (
            CardanoTransactionUtxoDTO.from_raw_cardano_tx_utxo(
                hash=dummy_transaction_utxo.hash, input=dummy_transaction_utxo
            )
        )

        assert dto.hash == dummy_transaction_utxo.hash
        assert isinstance(dto.created_at, datetime)
        inp_length: int = len(dto.inputs)
        i: int = 0
        a: int = 0
        while i < inp_length:
            assert dto.inputs[i].address == dummy_transaction_utxo.inputs[i].address
            assert (
                dto.inputs[i].tx_utxo_hash == dummy_transaction_utxo.inputs[i].tx_hash
            )
            assert (
                dto.inputs[i].output_index
                == dummy_transaction_utxo.inputs[i].output_index
            )
            assert dto.inputs[i].data_hash == dummy_transaction_utxo.inputs[i].data_hash
            assert (
                dto.inputs[i].inline_datum
                == dummy_transaction_utxo.inputs[i].inline_datum
            )
            assert (
                dto.inputs[i].reference_script_hash
                == dummy_transaction_utxo.inputs[i].reference_script_hash
            )
            assert (
                dto.inputs[i].collateral == dummy_transaction_utxo.inputs[i].collateral
            )
            inp_amount_len: int = len(dto.inputs[i].amounts)
            while a < inp_amount_len:
                assert (
                    dto.inputs[i].amounts[a].unit
                    == dummy_transaction_utxo.inputs[i].amount[a].unit
                )
                assert dto.inputs[i].amounts[a].quantity == Decimal(
                    dummy_transaction_utxo.inputs[i].amount[a].quantity
                )
                a += 1
            i += 1

        j: int = 0
        b: int = 0
        out_length: int = len(dto.outputs)
        while j < out_length:
            assert dto.outputs[j].address == dummy_transaction_utxo.outputs[j].address
            assert (
                dto.outputs[j].output_index
                == dummy_transaction_utxo.outputs[j].output_index
            )
            assert (
                dto.outputs[j].data_hash == dummy_transaction_utxo.outputs[j].data_hash
            )
            assert (
                dto.outputs[j].inline_datum
                == dummy_transaction_utxo.outputs[j].inline_datum
            )
            assert (
                dto.outputs[j].collateral
                == dummy_transaction_utxo.outputs[j].collateral
            )
            assert (
                dto.outputs[j].reference_script_hash
                == dummy_transaction_utxo.outputs[j].reference_script_hash
            )
            assert (
                dto.outputs[j].consumed_by_tx
                == dummy_transaction_utxo.outputs[j].consumed_by_tx
            )
            out_amount_len: int = len(dto.outputs[j].amounts)
            while b < out_amount_len:
                assert (
                    dto.inputs[j].amounts[b].unit
                    == dummy_transaction_utxo.inputs[j].amount[b].unit
                )
                assert dto.inputs[j].amounts[b].quantity == Decimal(
                    dummy_transaction_utxo.inputs[j].amount[b].quantity
                )
                b += 1
            j += 1

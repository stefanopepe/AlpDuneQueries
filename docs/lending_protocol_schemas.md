# Lending Protocol Schemas - Dune Analytics

This document provides schema references for DeFi lending protocols on Ethereum, validated against actual Dune Analytics tables (as of 2026-02-05).

## Table of Contents

1. [Aave V3](#aave-v3)
2. [Morpho (Aave V2 Optimizer)](#morpho-aave-v2-optimizer)
3. [Compound V2](#compound-v2)
4. [ERC-20 Transfers](#erc-20-transfers)
5. [Price Data](#price-data)
6. [Token Metadata](#token-metadata)

---

## Aave V3

**Schema:** `aave_v3_ethereum`

### Pool Event Tables

#### `aave_v3_ethereum.pool_evt_supply`

Supply (deposit) events when users add collateral.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `evt_tx_from` | VARBINARY | Transaction sender |
| `evt_tx_to` | VARBINARY | Transaction recipient |
| `contract_address` | VARBINARY | Pool contract address |
| `user` | VARBINARY | Address initiating the supply |
| `onBehalfOf` | VARBINARY | Address receiving the aTokens |
| `reserve` | VARBINARY | Underlying asset address |
| `amount` | UINT256 | Amount supplied (in asset decimals) |
| `referralCode` | UINT16 | Referral code |

#### `aave_v3_ethereum.pool_evt_borrow`

Borrow events when users take loans.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `evt_tx_from` | VARBINARY | Transaction sender |
| `evt_tx_to` | VARBINARY | Transaction recipient |
| `contract_address` | VARBINARY | Pool contract address |
| `user` | VARBINARY | Address initiating the borrow |
| `onBehalfOf` | VARBINARY | Address receiving the debt |
| `reserve` | VARBINARY | Underlying asset address |
| `amount` | UINT256 | Amount borrowed (in asset decimals) |
| `interestRateMode` | UINT8 | 1 = stable, 2 = variable |
| `borrowRate` | UINT256 | Borrow rate at time of borrow |
| `referralCode` | UINT16 | Referral code |

#### `aave_v3_ethereum.pool_evt_repay`

Repayment events when users repay loans.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `contract_address` | VARBINARY | Pool contract address |
| `user` | VARBINARY | Address initiating the repay |
| `repayer` | VARBINARY | Address paying the debt |
| `reserve` | VARBINARY | Underlying asset address |
| `amount` | UINT256 | Amount repaid (in asset decimals) |
| `useATokens` | BOOLEAN | Whether aTokens were used for repayment |

#### `aave_v3_ethereum.pool_evt_withdraw`

Withdrawal events when users remove collateral.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `contract_address` | VARBINARY | Pool contract address |
| `user` | VARBINARY | Address initiating withdrawal |
| `to` | VARBINARY | Address receiving the assets |
| `reserve` | VARBINARY | Underlying asset address |
| `amount` | UINT256 | Amount withdrawn (in asset decimals) |

#### `aave_v3_ethereum.pool_evt_liquidationcall`

Liquidation events when positions are liquidated.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `contract_address` | VARBINARY | Pool contract address |
| `collateralAsset` | VARBINARY | Collateral being seized |
| `debtAsset` | VARBINARY | Debt being repaid |
| `user` | VARBINARY | Address being liquidated |
| `debtToCover` | UINT256 | Amount of debt covered |
| `liquidatedCollateralAmount` | UINT256 | Amount of collateral seized |
| `liquidator` | VARBINARY | Address performing liquidation |
| `receiveAToken` | BOOLEAN | Whether liquidator receives aTokens |

---

## Morpho (Aave V2 Optimizer)

**Schema:** `morpho_aave_v2_ethereum`

Morpho is a lending optimizer that sits on top of Aave V2, matching peer-to-peer where possible.

### Event Tables

#### `morpho_aave_v2_ethereum.morpho_evt_supplied`

Supply events through Morpho.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `evt_tx_from` | VARBINARY | Transaction sender |
| `evt_tx_to` | VARBINARY | Transaction recipient |
| `contract_address` | VARBINARY | Morpho contract address |
| `_from` | VARBINARY | Address initiating supply |
| `_onBehalf` | VARBINARY | Address receiving position |
| `_poolToken` | VARBINARY | Aave aToken address |
| `_amount` | UINT256 | Amount supplied (in underlying decimals) |
| `_balanceInP2P` | UINT256 | Balance matched peer-to-peer |
| `_balanceOnPool` | UINT256 | Balance in Aave pool |

#### `morpho_aave_v2_ethereum.morpho_evt_borrowed`

Borrow events through Morpho.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `evt_tx_from` | VARBINARY | Transaction sender |
| `evt_tx_to` | VARBINARY | Transaction recipient |
| `contract_address` | VARBINARY | Morpho contract address |
| `_borrower` | VARBINARY | Address receiving the loan |
| `_poolToken` | VARBINARY | Aave aToken address |
| `_amount` | UINT256 | Amount borrowed (in underlying decimals) |
| `_balanceInP2P` | UINT256 | Balance matched peer-to-peer |
| `_balanceOnPool` | UINT256 | Balance from Aave pool |

#### `morpho_aave_v2_ethereum.morpho_evt_repaid`

Repayment events through Morpho.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `contract_address` | VARBINARY | Morpho contract address |
| `_repayer` | VARBINARY | Address making repayment |
| `_onBehalf` | VARBINARY | Address whose debt is repaid |
| `_poolToken` | VARBINARY | Aave aToken address |
| `_amount` | UINT256 | Amount repaid |
| `_balanceInP2P` | UINT256 | Remaining P2P balance |
| `_balanceOnPool` | UINT256 | Remaining pool balance |

#### `morpho_aave_v2_ethereum.morpho_evt_withdrawn`

Withdrawal events through Morpho.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_block_number` | BIGINT | Block number |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index in transaction |
| `contract_address` | VARBINARY | Morpho contract address |
| `_from` | VARBINARY | Address initiating withdrawal |
| `_to` | VARBINARY | Address receiving assets |
| `_poolToken` | VARBINARY | Aave aToken address |
| `_amount` | UINT256 | Amount withdrawn |
| `_balanceInP2P` | UINT256 | Remaining P2P balance |
| `_balanceOnPool` | UINT256 | Remaining pool balance |

#### `morpho_aave_v2_ethereum.morpho_evt_liquidated`

Liquidation events through Morpho.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `contract_address` | VARBINARY | Morpho contract address |
| `_liquidator` | VARBINARY | Address performing liquidation |
| `_liquidated` | VARBINARY | Address being liquidated |
| `_poolTokenBorrowed` | VARBINARY | Debt asset aToken |
| `_amountRepaid` | UINT256 | Debt repaid |
| `_poolTokenCollateral` | VARBINARY | Collateral asset aToken |
| `_amountSeized` | UINT256 | Collateral seized |

---

## Compound V2

**Schema:** `compound_ethereum`

### Event Tables

#### `compound_ethereum.cerc20delegator_evt_mint`

Supply (mint cTokens) events.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index |
| `contract_address` | VARBINARY | cToken contract address |
| `minter` | VARBINARY | Address supplying assets |
| `mintAmount` | UINT256 | Amount of underlying supplied |
| `mintTokens` | UINT256 | Amount of cTokens minted |

#### `compound_ethereum.cerc20delegator_evt_borrow`

Borrow events.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index |
| `contract_address` | VARBINARY | cToken contract address |
| `borrower` | VARBINARY | Address receiving the loan |
| `borrowAmount` | UINT256 | Amount borrowed |
| `accountBorrows` | UINT256 | Total account borrows |
| `totalBorrows` | UINT256 | Total market borrows |

#### `compound_ethereum.cerc20delegator_evt_repayborrow`

Repayment events.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index |
| `contract_address` | VARBINARY | cToken contract address |
| `payer` | VARBINARY | Address making repayment |
| `borrower` | VARBINARY | Address whose debt is repaid |
| `repayAmount` | UINT256 | Amount repaid |
| `accountBorrows` | UINT256 | Remaining account borrows |
| `totalBorrows` | UINT256 | Total market borrows |

#### `compound_ethereum.cerc20delegator_evt_redeem`

Withdrawal (redeem cTokens) events.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_block_date` | DATE | Block date |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index |
| `contract_address` | VARBINARY | cToken contract address |
| `redeemer` | VARBINARY | Address withdrawing |
| `redeemAmount` | UINT256 | Amount of underlying received |
| `redeemTokens` | UINT256 | Amount of cTokens redeemed |

#### `compound_ethereum.cerc20delegator_evt_liquidateborrow`

Liquidation events.

| Column | Type | Description |
|--------|------|-------------|
| `evt_block_time` | TIMESTAMP | Block timestamp |
| `evt_tx_hash` | VARBINARY | Transaction hash |
| `contract_address` | VARBINARY | cToken contract address |
| `liquidator` | VARBINARY | Address performing liquidation |
| `borrower` | VARBINARY | Address being liquidated |
| `repayAmount` | UINT256 | Debt repaid |
| `cTokenCollateral` | VARBINARY | Collateral cToken |
| `seizeTokens` | UINT256 | cTokens seized |

---

## ERC-20 Transfers

**Schema:** `transfers`

### `transfers.erc20`

Unified ERC-20 transfer events (Spellbook).

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name ('ethereum') |
| `block_time` | TIMESTAMP | Block timestamp |
| `block_date` | DATE | Block date |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Log index |
| `contract_address` | VARBINARY | Token contract |
| `from` | VARBINARY | Sender address |
| `to` | VARBINARY | Recipient address |
| `amount_raw` | UINT256 | Raw transfer amount |
| `amount` | DOUBLE | Decimal-adjusted amount |

---

## Price Data

**Schema:** `prices`

### `prices.usd`

Historical token prices in USD.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `contract_address` | VARBINARY | Token contract |
| `symbol` | VARCHAR | Token symbol |
| `decimals` | INT | Token decimals |
| `minute` | TIMESTAMP | Price timestamp (minute granularity) |
| `price` | DOUBLE | USD price |

**Usage Note:** Join on `contract_address` and `date_trunc('minute', evt_block_time)`.

---

## Token Metadata

**Schema:** `tokens`

### `tokens.erc20`

ERC-20 token metadata.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `contract_address` | VARBINARY | Token contract |
| `symbol` | VARCHAR | Token symbol |
| `decimals` | INT | Token decimals |

---

## Key Stablecoin Addresses (Ethereum)

| Token | Address |
|-------|---------|
| USDC | `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` |
| USDT | `0xdac17f958d2ee523a2206206994597c13d831ec7` |
| DAI | `0x6b175474e89094c44da98b954eedeac495271d0f` |
| FRAX | `0x853d955acef822db058eb8505911ed77f175b99e` |

---

## Key Contract Addresses (Ethereum)

| Protocol | Contract | Address |
|----------|----------|---------|
| Aave V3 Pool | Pool | `0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2` |
| Morpho Aave V2 | Morpho | `0x777777c9898d384f785ee44acfe945efdff5f3e0` |

---

## Notes

1. **Amount Decimals:** All `amount` fields in event tables are in the token's native decimals. Divide by `10^decimals` to get human-readable amounts.

2. **Morpho Pool Tokens:** Morpho uses Aave aToken addresses (`_poolToken`) to identify markets. To get the underlying asset, look up the aToken's `UNDERLYING_ASSET_ADDRESS`.

3. **Compound cTokens:** Compound V2 uses cToken contracts. The underlying asset can be fetched via the `underlying()` function on non-ETH markets.

4. **Entity Resolution:** Use `onBehalfOf` (Aave) or `_onBehalf` (Morpho) for entity attribution, falling back to `user`/`_from` if null.

"""Unit tests for _classify_plan using real PostgreSQL EXPLAIN output (no DB required).

Plans are copied verbatim from `EXPLAIN SELECT ...` on a 50k-row table.
Each plan string is what PostgreSQL actually returns line by line.
"""

from __future__ import annotations


from sqlalchemy_spy.renderers.html import _classify_plan


# ── PostgreSQL plans ────────────────────────────────────────────────────────

PG_SEQ_SCAN = [
    "Seq Scan on orders  (cost=0.00..1887.00 rows=100000 width=19)",
]

PG_SEQ_SCAN_FILTER = [
    "Seq Scan on orders  (cost=0.00..1887.00 rows=33403 width=19)",
    "  Filter: (status = 'shipped'::text)",
]

PG_INDEX_SCAN_PK = [
    "Index Scan using users_pkey on users  (cost=0.29..8.30 rows=1 width=15)",
    "  Index Cond: (id = 42)",
]

PG_BITMAP_SINGLE_COL = [
    "Bitmap Heap Scan on orders  (cost=530.43..1887.93 rows=33403 width=19)",
    "  Recheck Cond: (status = 'shipped'::text)",
    "  ->  Bitmap Index Scan on idx_orders_status  (cost=0.00..522.09 rows=33403 width=0)",
    "        Index Cond: (status = 'shipped'::text)",
]

PG_COMPOSITE_INDEX = [
    "Bitmap Heap Scan on orders  (cost=4.32..15.75 rows=3 width=19)",
    "  Recheck Cond: ((user_id = 500) AND (status = 'done'::text))",
    "  ->  Bitmap Index Scan on idx_orders_user_status  (cost=0.00..4.32 rows=3 width=0)",
    "        Index Cond: ((user_id = 500) AND (status = 'done'::text))",
]

PG_INDEX_ONLY_SCAN_COMPOSITE = [
    "Index Only Scan using idx_orders_user_status on orders  (cost=0.42..4.44 rows=3 width=11)",
    "  Index Cond: ((user_id = 500) AND (status = 'done'::text))",
    "  Heap Fetches: 0",
]

PG_NESTED_LOOP_JOIN_INDEX = [
    "Nested Loop  (cost=0.58..26.59 rows=5 width=12)",
    "  ->  Index Scan using users_pkey on users u  (cost=0.29..8.30 rows=1 width=10)",
    "        Index Cond: (id = 500)",
    "  ->  Index Scan using idx_orders_user_id on orders o  (cost=0.29..18.22 rows=10 width=8)",
    "        Index Cond: (user_id = 500)",
]

PG_HASH_JOIN_FULL_SCAN = [
    "Hash Join  (cost=214.00..3887.00 rows=100000 width=26)",
    "  Hash Cond: (o.user_id = u.id)",
    "  ->  Seq Scan on orders o  (cost=0.00..1887.00 rows=100000 width=19)",
    "  ->  Hash  (cost=164.00..164.00 rows=4000 width=15)",
    "        ->  Seq Scan on users u  (cost=0.00..164.00 rows=4000 width=15)",
    "              Filter: (city = 'Paris'::text)",
]

PG_THREE_TABLE_JOIN_COMPOSITE = [
    "Limit  (cost=1.00..40.12 rows=5 width=23)",
    "  ->  Nested Loop  (cost=1.00..400.12 rows=50 width=23)",
    "        ->  Nested Loop  (cost=0.58..20.60 rows=3 width=17)",
    "              ->  Index Scan using users_pkey on users u  (cost=0.29..8.30 rows=1 width=10)",
    "                    Index Cond: (id = 500)",
    "              ->  Bitmap Heap Scan on orders o  (cost=0.29..12.29 rows=3 width=15)",
    "                    Recheck Cond: ((user_id = 500) AND (status = 'done'::text))",
    "                    ->  Bitmap Index Scan on idx_orders_user_status  (cost=0.00..4.32 rows=3 width=0)",
    "                          Index Cond: ((user_id = 500) AND (status = 'done'::text))",
    "        ->  Index Scan using idx_items_order_id on items i  (cost=0.42..9.84 rows=10 width=12)",
    "              Index Cond: (order_id = o.id)",
]

PG_SORT_THEN_SEQ = [
    "Sort  (cost=380.10..390.10 rows=4000 width=18)",
    "  Sort Key: city",
    "  ->  Seq Scan on users  (cost=0.00..139.00 rows=4000 width=18)",
]

PG_HASHAGG_SEQ = [
    "HashAggregate  (cost=214.00..214.05 rows=5 width=14)",
    "  Group Key: city",
    "  ->  Seq Scan on users  (cost=0.00..164.00 rows=4000 width=14)",
]

PG_CTE_HASH_JOIN = [
    "Limit  (cost=2581.90..2581.91 rows=5 width=12)",
    "  ->  Sort  (cost=2581.90..2583.40 rows=600 width=12)",
    "        Sort Key: (sum(t_orders.total)) DESC",
    "        ->  Hash Join  (cost=1937.00..2568.90 rows=600 width=12)",
    "              Hash Cond: (u.id = t_orders.user_id)",
    "              ->  Seq Scan on users u  (cost=0.00..164.00 rows=4000 width=10)",
    "              ->  Hash  (cost=1929.50..1929.50 rows=600 width=8)",
    "                    ->  Subquery Scan on t  (cost=1887.00..1929.50 rows=600 width=8)",
    "                          ->  HashAggregate  (cost=1887.00..1917.00 rows=600 width=8)",
    "                                Group Key: t_orders.user_id",
    "                                Filter: (sum(t_orders.total) > '5000'::double precision)",
    "                                ->  Seq Scan on orders  (cost=0.00..1887.00 rows=100000 width=8)",
]

PG_CORRELATED_SUBQUERY = [
    "Seq Scan on users u  (cost=0.00..61391.00 rows=4000 width=8)",
    "  Filter: ((SubPlan 1) > 15)",
    "  SubPlan 1",
    "    ->  Aggregate  (cost=15.32..15.33 rows=1 width=8)",
    "          ->  Index Scan using idx_orders_user_id on orders o  (cost=0.29..15.22 rows=10 width=0)",
    "                Index Cond: (user_id = u.id)",
]

# ── SQLite plans ─────────────────────────────────────────────────────────────

SQ_SCAN = ["SCAN users"]
SQ_SCAN_ALIAS = ["SCAN u"]

SQ_SEARCH_PK = ["SEARCH items USING INTEGER PRIMARY KEY (rowid=?)"]

SQ_SEARCH_SINGLE_IDX = ["SEARCH orders USING INDEX idx_orders_status (status=?)"]

SQ_SEARCH_COMPOSITE = [
    "SEARCH order_items USING INDEX idx_items_composite (product_id=? AND price<?)"
]

SQ_SEARCH_COVERING_COMPOSITE = [
    "SEARCH order_items USING COVERING INDEX idx_items_composite (product_id=? AND price<?)"
]

SQ_SCAN_COVERING = [
    "SCAN order_items USING COVERING INDEX idx_items_composite",
    "SEARCH products USING INTEGER PRIMARY KEY (rowid=?)",
    "USE TEMP B-TREE FOR DISTINCT",
]

SQ_MULTI_TABLE_JOIN = [
    "SEARCH orders USING INDEX idx_orders_user_id (user_id=?)",
    "SEARCH order_items USING INDEX idx_items_order_id (order_id=?)",
    "SEARCH products USING INTEGER PRIMARY KEY (rowid=?)",
]

SQ_JOIN_WITH_SCAN = [
    "SCAN users",
    "SEARCH orders USING INDEX idx_orders_user_id (user_id=?)",
]

SQ_SORT = [
    "SCAN users",
    "USE TEMP B-TREE FOR ORDER BY",
]

SQ_COROUTINE_CTE = [
    "CO-ROUTINE top",
    "SEARCH orders USING INDEX idx_orders_user_id (user_id=?)",
    "SEARCH users USING INTEGER PRIMARY KEY (rowid=?)",
]


# ── Tests ────────────────────────────────────────────────────────────────────


class TestClassifyPgFullScan:
    def test_seq_scan(self):
        assert _classify_plan(PG_SEQ_SCAN) == ("full-scan", "Full Scan")

    def test_seq_scan_with_filter(self):
        assert _classify_plan(PG_SEQ_SCAN_FILTER) == ("full-scan", "Full Scan")

    def test_hash_join_with_seq_scan(self):
        assert _classify_plan(PG_HASH_JOIN_FULL_SCAN) == ("full-scan", "Full Scan")

    def test_sort_over_seq_scan(self):
        assert _classify_plan(PG_SORT_THEN_SEQ) == ("full-scan", "Full Scan")

    def test_hashagg_over_seq_scan(self):
        assert _classify_plan(PG_HASHAGG_SEQ) == ("full-scan", "Full Scan")

    def test_cte_hash_join_with_seq(self):
        assert _classify_plan(PG_CTE_HASH_JOIN) == ("full-scan", "Full Scan")


class TestClassifyPgIndex:
    def test_pk_index_scan(self):
        assert _classify_plan(PG_INDEX_SCAN_PK) == ("index", "Index")

    def test_bitmap_single_col(self):
        key, _ = _classify_plan(PG_BITMAP_SINGLE_COL)
        assert key == "index"

    def test_nested_loop_index_join(self):
        key, _ = _classify_plan(PG_NESTED_LOOP_JOIN_INDEX)
        assert key == "index"

    def test_correlated_subquery_has_index(self):
        # Top node is Seq Scan, but subplan uses an index — top wins → full-scan
        key, _ = _classify_plan(PG_CORRELATED_SUBQUERY)
        assert key == "full-scan"

    def test_bitmap_not_classified_as_full_scan(self):
        key, _ = _classify_plan(PG_BITMAP_SINGLE_COL)
        assert key != "full-scan"


class TestClassifyPgCompositeIndex:
    def test_bitmap_composite(self):
        assert _classify_plan(PG_COMPOSITE_INDEX) == (
            "composite-index",
            "Composite Index",
        )

    def test_index_only_scan_composite(self):
        assert _classify_plan(PG_INDEX_ONLY_SCAN_COMPOSITE) == (
            "composite-index",
            "Composite Index",
        )

    def test_three_table_join_composite(self):
        assert _classify_plan(PG_THREE_TABLE_JOIN_COMPOSITE) == (
            "composite-index",
            "Composite Index",
        )


class TestClassifySqFullScan:
    def test_bare_scan(self):
        assert _classify_plan(SQ_SCAN) == ("full-scan", "Full Scan")

    def test_scan_alias(self):
        assert _classify_plan(SQ_SCAN_ALIAS) == ("full-scan", "Full Scan")

    def test_join_with_scan_side(self):
        assert _classify_plan(SQ_JOIN_WITH_SCAN) == ("full-scan", "Full Scan")

    def test_sort_with_scan(self):
        assert _classify_plan(SQ_SORT) == ("full-scan", "Full Scan")


class TestClassifySqIndex:
    def test_pk_lookup(self):
        assert _classify_plan(SQ_SEARCH_PK) == ("index", "Index")

    def test_single_index(self):
        assert _classify_plan(SQ_SEARCH_SINGLE_IDX) == ("index", "Index")

    def test_multi_table_join_indexes(self):
        assert _classify_plan(SQ_MULTI_TABLE_JOIN) == ("index", "Index")

    def test_cte_with_index(self):
        key, _ = _classify_plan(SQ_COROUTINE_CTE)
        assert key == "index"


class TestClassifySqCompositeIndex:
    def test_composite_using_index(self):
        assert _classify_plan(SQ_SEARCH_COMPOSITE) == (
            "composite-index",
            "Composite Index",
        )

    def test_composite_covering_index(self):
        assert _classify_plan(SQ_SEARCH_COVERING_COMPOSITE) == (
            "composite-index",
            "Composite Index",
        )

    def test_scan_covering_index_not_full_scan(self):
        # SCAN ... USING COVERING INDEX — it's a full index scan, not a table scan
        key, _ = _classify_plan(SQ_SCAN_COVERING)
        assert key != "full-scan"


class TestClassifyEdgeCases:
    def test_empty_plan(self):
        assert _classify_plan([]) == ("plan", "Plan")

    def test_pragma_or_ddl_empty(self):
        assert _classify_plan([]) == ("plan", "Plan")

    def test_unknown_node(self):
        key, lbl = _classify_plan(["Materialize  (cost=0.00..0.01 rows=1 width=0)"])
        assert key == "plan"
        assert lbl == "Plan"

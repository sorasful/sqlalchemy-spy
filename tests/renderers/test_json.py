import json


from sqlalchemy_spy import JsonRenderer


class TestJsonRendererStructure:
    def test_render_returns_valid_json(self, profiler_with_queries):
        output = JsonRenderer().render(profiler_with_queries)
        data = json.loads(output)
        assert isinstance(data, dict)

    def test_top_level_fields_present(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert "query_count" in data
        assert "total_ms" in data
        assert "avg_ms" in data
        assert "queries" in data

    def test_queries_is_list(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert isinstance(data["queries"], list)

    def test_query_count_matches(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert data["query_count"] == 2
        assert len(data["queries"]) == 2


class TestJsonRendererQueryFields:
    def test_required_fields_on_each_query(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        for q in data["queries"]:
            assert "statement" in q
            assert "params" in q
            assert "operation" in q
            assert "duration_ms" in q
            assert "error" in q
            assert "call_site" in q

    def test_operation_is_select(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        for q in data["queries"]:
            assert q["operation"] == "SELECT"

    def test_duration_ms_is_positive_float(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        for q in data["queries"]:
            assert isinstance(q["duration_ms"], float)
            assert q["duration_ms"] > 0

    def test_error_is_none_when_no_error(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        for q in data["queries"]:
            assert q["error"] is None

    def test_statement_content(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        statements = [q["statement"] for q in data["queries"]]
        assert any("items" in s for s in statements)


class TestJsonRendererCallSite:
    def test_call_site_fields(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        call_site = data["queries"][0]["call_site"]
        assert call_site is not None
        assert "file" in call_site
        assert "line" in call_site
        assert "function" in call_site

    def test_call_site_line_is_int(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert isinstance(data["queries"][0]["call_site"]["line"], int)

    def test_call_site_none_when_no_stack(self, profiler_no_stack):
        data = json.loads(JsonRenderer().render(profiler_no_stack))
        assert data["queries"][0]["call_site"] is None


class TestJsonRendererAggregates:
    def test_total_ms_positive(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert data["total_ms"] > 0

    def test_avg_ms_is_positive_and_at_most_total(self, profiler_with_queries):
        data = json.loads(JsonRenderer().render(profiler_with_queries))
        assert data["avg_ms"] > 0
        assert data["avg_ms"] <= data["total_ms"]

    def test_empty_profiler_zero_values(self, empty_profiler):
        data = json.loads(JsonRenderer().render(empty_profiler))
        assert data["query_count"] == 0
        assert data["total_ms"] == 0
        assert data["avg_ms"] == 0
        assert data["queries"] == []


class TestJsonRendererError:
    def test_error_field_populated_on_failed_query(self, profiler_with_error):
        data = json.loads(JsonRenderer().render(profiler_with_error))
        assert data["query_count"] == 1
        assert data["queries"][0]["error"] is not None
        assert isinstance(data["queries"][0]["error"], str)
        assert len(data["queries"][0]["error"]) > 0


class TestJsonRendererOptions:
    def test_default_indent_produces_multiline(self, profiler_with_queries):
        output = JsonRenderer().render(profiler_with_queries)
        assert "\n" in output

    def test_indent_none_produces_compact(self, profiler_with_queries):
        output = JsonRenderer(indent=None).render(profiler_with_queries)
        assert "\n" not in output

    def test_print_outputs_valid_json_to_stdout(self, profiler_with_queries, capsys):
        JsonRenderer().print(profiler_with_queries)
        output = capsys.readouterr().out
        data = json.loads(output)
        assert data["query_count"] == 2

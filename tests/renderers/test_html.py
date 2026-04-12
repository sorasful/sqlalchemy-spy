from sqlalchemy_spy import HtmlRenderer


class TestHtmlRendererStructure:
    def test_render_returns_string(self, profiler_with_queries):
        assert isinstance(HtmlRenderer().render(profiler_with_queries), str)

    def test_valid_html_doctype(self, profiler_with_queries):
        assert (
            HtmlRenderer().render(profiler_with_queries).startswith("<!DOCTYPE html>")
        )

    def test_title_contains_query_count(self, profiler_with_queries):
        assert "2 queries" in HtmlRenderer().render(profiler_with_queries)

    def test_title_singular(self, profiler_single_query):
        assert "1 query" in HtmlRenderer().render(profiler_single_query)

    def test_contains_charset_meta(self, profiler_with_queries):
        assert 'charset="UTF-8"' in HtmlRenderer().render(profiler_with_queries)

    def test_embedded_css(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert "<style>" in html and "</style>" in html

    def test_embedded_js(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert "<script>" in html and "</script>" in html


class TestHtmlRendererContent:
    def test_query_rows_present(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert html.count('class="qrow"') == 2

    def test_detail_rows_match_query_rows(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert html.count('class="detail hidden"') == 2

    def test_operation_badges(self, profiler_with_queries):
        assert "op-SELECT" in HtmlRenderer().render(profiler_with_queries)

    def test_sql_keywords_highlighted(self, profiler_with_queries):
        assert 'class="kw"' in HtmlRenderer().render(profiler_with_queries)

    def test_call_site_shown(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert "cs-file" in html
        assert "cs-fn" in html

    def test_error_badge_shown(self, profiler_with_error):
        assert "err-badge" in HtmlRenderer().render(profiler_with_error)

    def test_no_call_site_when_no_stack(self, profiler_no_stack):
        html = HtmlRenderer().render(profiler_no_stack)
        assert '<span class="cs-file"' not in html


class TestHtmlRendererHotPaths:
    def test_hot_paths_sections_present(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert "hp-count" in html
        assert "hp-time" in html

    def test_hot_paths_absent_when_no_stack(self, profiler_no_stack):
        html = HtmlRenderer().render(profiler_no_stack)
        assert "hp-count" not in html
        assert "hp-time" not in html

    def test_hot_paths_absent_for_empty_profiler(self, empty_profiler):
        html = HtmlRenderer().render(empty_profiler)
        assert "hp-count" not in html


class TestHtmlRendererFilters:
    def test_filter_bar_present(self, profiler_with_queries):
        assert 'class="filters"' in HtmlRenderer().render(profiler_with_queries)

    def test_all_filter_button_present(self, profiler_with_queries):
        html = HtmlRenderer().render(profiler_with_queries)
        assert "All (" in html

    def test_select_filter_button_present(self, profiler_with_queries):
        assert "SELECT (" in HtmlRenderer().render(profiler_with_queries)


class TestHtmlRendererSave:
    def test_save_writes_file(self, profiler_with_queries, tmp_path):
        out = tmp_path / "report.html"
        result = HtmlRenderer().save(profiler_with_queries, out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_saved_file_is_valid_html(self, profiler_with_queries, tmp_path):
        out = tmp_path / "report.html"
        HtmlRenderer().save(profiler_with_queries, out)
        content = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
        assert 'class="qrow"' in content

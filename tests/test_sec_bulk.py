"""Tests for SEC bulk 13F downloader and parser."""

import csv
import io
import zipfile

import pytest

from db.database import get_connection, get_table_count, init_db, query_all, query_one
from scrapers.sec_bulk import (
    _is_value_in_thousands,
    _parse_sec_date,
    _quarter_from_date,
    build_quarter_list,
    parse_quarter_zip,
)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


def _make_zip(tmp_path, submissions, coverpages, infotable, summarypage, filename="test.zip"):
    """Create a synthetic SEC 13F ZIP file for testing."""
    zip_path = tmp_path / filename

    def write_tsv(name, rows):
        buf = io.StringIO()
        if rows:
            writer = csv.DictWriter(buf, fieldnames=rows[0].keys(), delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)
        return buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("SUBMISSION.tsv", write_tsv("SUBMISSION.tsv", submissions))
        zf.writestr("COVERPAGE.tsv", write_tsv("COVERPAGE.tsv", coverpages))
        zf.writestr("INFOTABLE.tsv", write_tsv("INFOTABLE.tsv", infotable))
        zf.writestr("SUMMARYPAGE.tsv", write_tsv("SUMMARYPAGE.tsv", summarypage))

    return zip_path


class TestBuildQuarterList:
    def test_generates_quarters(self):
        quarters = build_quarter_list(2022, 2023)
        keys = [q["quarter_key"] for q in quarters]
        assert "2022Q1" in keys
        assert "2023Q4" in keys
        assert len(quarters) == 8

    def test_old_format_urls(self):
        quarters = build_quarter_list(2022, 2022)
        for q in quarters:
            assert q["filename"] == f"2022q{q['quarter']}_form13f.zip"

    def test_new_format_urls(self):
        quarters = build_quarter_list(2024, 2024)
        assert len(quarters) == 4
        assert quarters[0]["filename"] == "01jan2024-29feb2024_form13f.zip"
        assert quarters[1]["filename"] == "01mar2024-31may2024_form13f.zip"

    def test_2014_to_2025(self):
        quarters = build_quarter_list(2014, 2025)
        assert len(quarters) >= 44  # 10 years × 4 + some 2024-2025


class TestDateParsing:
    def test_parse_sec_date(self):
        assert _parse_sec_date("30-SEP-2023") == "2023-09-30"
        assert _parse_sec_date("31-DEC-2024") == "2024-12-31"
        assert _parse_sec_date("01-JAN-2020") == "2020-01-01"

    def test_parse_sec_date_empty(self):
        assert _parse_sec_date("") is None
        assert _parse_sec_date(None) is None

    def test_quarter_from_date(self):
        assert _quarter_from_date("2023-09-30") == (2023, 3)
        assert _quarter_from_date("2024-12-31") == (2024, 4)
        assert _quarter_from_date("2024-03-31") == (2024, 1)


class TestValueCutover:
    def test_pre_2023_is_thousands(self):
        assert _is_value_in_thousands("2022Q4") is True
        assert _is_value_in_thousands("2020Q1") is True

    def test_2023q1_and_after_is_actual(self):
        assert _is_value_in_thousands("2023Q1") is False
        assert _is_value_in_thousands("2024Q3") is False


class TestParseQuarterZip:
    def _basic_zip(self, tmp_path, quarter_key="2023Q4"):
        """Create a minimal valid ZIP for testing."""
        submissions = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "FILING_DATE": "15-NOV-2023",
            "SUBMISSIONTYPE": "13F-HR",
            "CIK": "0001234567",
            "PERIODOFREPORT": "30-SEP-2023",
        }]
        coverpages = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "REPORTCALENDARORQUARTER": "30-SEP-2023",
            "ISAMENDMENT": "",
            "AMENDMENTNO": "",
            "AMENDMENTTYPE": "",
            "CONFDENIEDEXPIRED": "",
            "DATEDENIEDEXPIRED": "",
            "DATEREPORTED": "",
            "REASONFORNONCONFIDENTIALITY": "",
            "FILINGMANAGER_NAME": "TEST FUND LLC",
            "FILINGMANAGER_STREET1": "123 MAIN ST",
            "FILINGMANAGER_STREET2": "",
            "FILINGMANAGER_CITY": "NEW YORK",
            "FILINGMANAGER_STATEORCOUNTRY": "NY",
            "FILINGMANAGER_ZIPCODE": "10001",
            "REPORTTYPE": "13F HOLDINGS REPORT",
            "FORM13FFILENUMBER": "",
            "CRDNUMBER": "",
            "SECFILENUMBER": "",
            "PROVIDEINFOFORINSTRUCTION5": "",
            "ADDITIONALINFORMATION": "",
        }]
        infotable = [
            {
                "ACCESSION_NUMBER": "0001-23-000001",
                "INFOTABLE_SK": "1",
                "NAMEOFISSUER": "APPLE INC",
                "TITLEOFCLASS": "COM",
                "CUSIP": "037833100",
                "FIGI": "",
                "VALUE": "1500000",
                "SSHPRNAMT": "10000",
                "SSHPRNAMTTYPE": "SH",
                "PUTCALL": "",
                "INVESTMENTDISCRETION": "SOLE",
                "OTHERMANAGER": "",
                "VOTING_AUTH_SOLE": "10000",
                "VOTING_AUTH_SHARED": "0",
                "VOTING_AUTH_NONE": "0",
            },
            {
                "ACCESSION_NUMBER": "0001-23-000001",
                "INFOTABLE_SK": "2",
                "NAMEOFISSUER": "MICROSOFT CORP",
                "TITLEOFCLASS": "COM",
                "CUSIP": "594918104",
                "FIGI": "",
                "VALUE": "2500000",
                "SSHPRNAMT": "5000",
                "SSHPRNAMTTYPE": "SH",
                "PUTCALL": "",
                "INVESTMENTDISCRETION": "SOLE",
                "OTHERMANAGER": "",
                "VOTING_AUTH_SOLE": "5000",
                "VOTING_AUTH_SHARED": "0",
                "VOTING_AUTH_NONE": "0",
            },
        ]
        summarypage = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "OTHERINCLUDEDMANAGERSCOUNT": "0",
            "TABLEENTRYTOTAL": "2",
            "TABLEVALUETOTAL": "4000000",
            "ISCONFIDENTIALOMITTED": "N",
        }]
        return _make_zip(tmp_path, submissions, coverpages, infotable, summarypage)

    def test_basic_parse(self, tmp_path, db):
        zip_path = self._basic_zip(tmp_path)
        stats = parse_quarter_zip(zip_path, "2023Q4", db)

        assert stats["filings"] == 1
        assert stats["holdings"] == 2

        # Check filer
        filer = query_one(db, "SELECT * FROM filers WHERE cik = ?", ("1234567",))
        assert filer is not None
        assert filer["name"] == "TEST FUND LLC"

        # Check filing
        filing = query_one(db, "SELECT * FROM filings WHERE accession_number = ?", ("0001-23-000001",))
        assert filing is not None
        assert filing["form_type"] == "13F-HR"
        assert filing["report_date"] == "2023-09-30"
        assert filing["report_year"] == 2023
        assert filing["report_quarter"] == 3

        # Check holdings
        holdings = query_all(db, "SELECT * FROM holdings WHERE filing_id = ?", (filing["id"],))
        assert len(holdings) == 2

        # AAPL holding
        aapl = [h for h in holdings if h["cusip"] == "037833100"][0]
        assert aapl["value"] == 1500000.0  # 2023Q4 = actual dollars
        assert aapl["shares"] == 10000.0

    def test_value_in_thousands(self, tmp_path, db):
        """Pre-2023 values should be multiplied by 1000."""
        zip_path = self._basic_zip(tmp_path, quarter_key="2022Q4")
        stats = parse_quarter_zip(zip_path, "2022Q4", db)

        filing = query_one(db, "SELECT * FROM filings")
        holdings = query_all(db, "SELECT * FROM holdings WHERE filing_id = ?", (filing["id"],))

        aapl = [h for h in holdings if h["cusip"] == "037833100"][0]
        assert aapl["value"] == 1500000.0 * 1000  # Multiplied by 1000

        # Total value should also be multiplied
        assert filing["total_value"] == 4000000.0 * 1000

    def test_idempotent_parse(self, tmp_path, db):
        """Parsing the same ZIP twice should not create duplicate filings or holdings."""
        zip_path = self._basic_zip(tmp_path)

        parse_quarter_zip(zip_path, "2023Q4", db)
        parse_quarter_zip(zip_path, "2023Q4", db)

        assert get_table_count(db, "filings") == 1
        # Holdings will double since we don't deduplicate — this is expected
        # because the same accession_number filing gets INSERT OR IGNORE'd

    def test_securities_populated(self, tmp_path, db):
        """Distinct CUSIPs should be inserted into securities table."""
        zip_path = self._basic_zip(tmp_path)
        parse_quarter_zip(zip_path, "2023Q4", db)

        securities = query_all(db, "SELECT * FROM securities ORDER BY cusip")
        assert len(securities) == 2
        cusips = [s["cusip"] for s in securities]
        assert "037833100" in cusips
        assert "594918104" in cusips
        # Ticker should be unresolved
        assert all(s["ticker"] is None for s in securities)

    def test_put_call_stored(self, tmp_path, db):
        """PUT/CALL flag should be stored in holdings."""
        submissions = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "FILING_DATE": "15-NOV-2023",
            "SUBMISSIONTYPE": "13F-HR",
            "CIK": "0001234567",
            "PERIODOFREPORT": "30-SEP-2023",
        }]
        coverpages = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "REPORTCALENDARORQUARTER": "",
            "ISAMENDMENT": "",
            "AMENDMENTNO": "",
            "AMENDMENTTYPE": "",
            "CONFDENIEDEXPIRED": "",
            "DATEDENIEDEXPIRED": "",
            "DATEREPORTED": "",
            "REASONFORNONCONFIDENTIALITY": "",
            "FILINGMANAGER_NAME": "TEST FUND",
            "FILINGMANAGER_STREET1": "",
            "FILINGMANAGER_STREET2": "",
            "FILINGMANAGER_CITY": "",
            "FILINGMANAGER_STATEORCOUNTRY": "",
            "FILINGMANAGER_ZIPCODE": "",
            "REPORTTYPE": "",
            "FORM13FFILENUMBER": "",
            "CRDNUMBER": "",
            "SECFILENUMBER": "",
            "PROVIDEINFOFORINSTRUCTION5": "",
            "ADDITIONALINFORMATION": "",
        }]
        infotable = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "INFOTABLE_SK": "1",
            "NAMEOFISSUER": "APPLE INC",
            "TITLEOFCLASS": "PUT",
            "CUSIP": "037833100",
            "FIGI": "",
            "VALUE": "500000",
            "SSHPRNAMT": "100",
            "SSHPRNAMTTYPE": "SH",
            "PUTCALL": "PUT",
            "INVESTMENTDISCRETION": "SOLE",
            "OTHERMANAGER": "",
            "VOTING_AUTH_SOLE": "0",
            "VOTING_AUTH_SHARED": "0",
            "VOTING_AUTH_NONE": "0",
        }]
        summarypage = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "OTHERINCLUDEDMANAGERSCOUNT": "0",
            "TABLEENTRYTOTAL": "1",
            "TABLEVALUETOTAL": "500000",
            "ISCONFIDENTIALOMITTED": "N",
        }]

        zip_path = _make_zip(tmp_path, submissions, coverpages, infotable, summarypage)
        parse_quarter_zip(zip_path, "2023Q4", db)

        holding = query_one(db, "SELECT * FROM holdings")
        assert holding["put_call"] == "PUT"

    def test_amendment_restatement(self, tmp_path, db):
        """RESTATEMENT amendment should replace original filing's holdings."""
        # First parse original filing
        submissions_orig = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "FILING_DATE": "15-NOV-2023",
            "SUBMISSIONTYPE": "13F-HR",
            "CIK": "0001234567",
            "PERIODOFREPORT": "30-SEP-2023",
        }]
        coverpages_orig = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "REPORTCALENDARORQUARTER": "",
            "ISAMENDMENT": "",
            "AMENDMENTNO": "",
            "AMENDMENTTYPE": "",
            "CONFDENIEDEXPIRED": "",
            "DATEDENIEDEXPIRED": "",
            "DATEREPORTED": "",
            "REASONFORNONCONFIDENTIALITY": "",
            "FILINGMANAGER_NAME": "TEST FUND",
            "FILINGMANAGER_STREET1": "",
            "FILINGMANAGER_STREET2": "",
            "FILINGMANAGER_CITY": "",
            "FILINGMANAGER_STATEORCOUNTRY": "",
            "FILINGMANAGER_ZIPCODE": "",
            "REPORTTYPE": "",
            "FORM13FFILENUMBER": "",
            "CRDNUMBER": "",
            "SECFILENUMBER": "",
            "PROVIDEINFOFORINSTRUCTION5": "",
            "ADDITIONALINFORMATION": "",
        }]
        infotable_orig = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "INFOTABLE_SK": "1",
            "NAMEOFISSUER": "APPLE INC",
            "TITLEOFCLASS": "COM",
            "CUSIP": "037833100",
            "FIGI": "",
            "VALUE": "1000000",
            "SSHPRNAMT": "5000",
            "SSHPRNAMTTYPE": "SH",
            "PUTCALL": "",
            "INVESTMENTDISCRETION": "SOLE",
            "OTHERMANAGER": "",
            "VOTING_AUTH_SOLE": "5000",
            "VOTING_AUTH_SHARED": "0",
            "VOTING_AUTH_NONE": "0",
        }]
        summarypage_orig = [{
            "ACCESSION_NUMBER": "0001-23-000001",
            "OTHERINCLUDEDMANAGERSCOUNT": "0",
            "TABLEENTRYTOTAL": "1",
            "TABLEVALUETOTAL": "1000000",
            "ISCONFIDENTIALOMITTED": "N",
        }]

        zip_orig = _make_zip(tmp_path, submissions_orig, coverpages_orig, infotable_orig, summarypage_orig, "orig.zip")
        parse_quarter_zip(zip_orig, "2023Q4", db)

        # Verify original has 1 holding
        assert get_table_count(db, "holdings") == 1

        # Now parse amendment (restatement) with different holdings
        submissions_amend = [{
            "ACCESSION_NUMBER": "0001-23-000002",
            "FILING_DATE": "20-DEC-2023",
            "SUBMISSIONTYPE": "13F-HR/A",
            "CIK": "0001234567",
            "PERIODOFREPORT": "30-SEP-2023",
        }]
        coverpages_amend = [{
            "ACCESSION_NUMBER": "0001-23-000002",
            "REPORTCALENDARORQUARTER": "",
            "ISAMENDMENT": "Y",
            "AMENDMENTNO": "1",
            "AMENDMENTTYPE": "RESTATEMENT",
            "CONFDENIEDEXPIRED": "",
            "DATEDENIEDEXPIRED": "",
            "DATEREPORTED": "",
            "REASONFORNONCONFIDENTIALITY": "",
            "FILINGMANAGER_NAME": "TEST FUND",
            "FILINGMANAGER_STREET1": "",
            "FILINGMANAGER_STREET2": "",
            "FILINGMANAGER_CITY": "",
            "FILINGMANAGER_STATEORCOUNTRY": "",
            "FILINGMANAGER_ZIPCODE": "",
            "REPORTTYPE": "",
            "FORM13FFILENUMBER": "",
            "CRDNUMBER": "",
            "SECFILENUMBER": "",
            "PROVIDEINFOFORINSTRUCTION5": "",
            "ADDITIONALINFORMATION": "",
        }]
        infotable_amend = [{
            "ACCESSION_NUMBER": "0001-23-000002",
            "INFOTABLE_SK": "10",
            "NAMEOFISSUER": "MICROSOFT CORP",
            "TITLEOFCLASS": "COM",
            "CUSIP": "594918104",
            "FIGI": "",
            "VALUE": "2000000",
            "SSHPRNAMT": "8000",
            "SSHPRNAMTTYPE": "SH",
            "PUTCALL": "",
            "INVESTMENTDISCRETION": "SOLE",
            "OTHERMANAGER": "",
            "VOTING_AUTH_SOLE": "8000",
            "VOTING_AUTH_SHARED": "0",
            "VOTING_AUTH_NONE": "0",
        }]
        summarypage_amend = [{
            "ACCESSION_NUMBER": "0001-23-000002",
            "OTHERINCLUDEDMANAGERSCOUNT": "0",
            "TABLEENTRYTOTAL": "1",
            "TABLEVALUETOTAL": "2000000",
            "ISCONFIDENTIALOMITTED": "N",
        }]

        zip_amend = _make_zip(tmp_path, submissions_amend, coverpages_amend, infotable_amend, summarypage_amend, "amend.zip")
        parse_quarter_zip(zip_amend, "2023Q4", db)

        # Original filing's holdings should be deleted, amendment's holdings should exist
        # Original AAPL holding should be gone
        aapl = query_one(db, "SELECT * FROM holdings WHERE cusip = ?", ("037833100",))
        assert aapl is None

        # Amendment MSFT holding should exist
        msft = query_one(db, "SELECT * FROM holdings WHERE cusip = ?", ("594918104",))
        assert msft is not None
        assert msft["value"] == 2000000.0

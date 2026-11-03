
import requests
import time
import json

# ==========================================
# KONFIGURASI ELASTICSEARCH
# ==========================================

ES_HOST = "http://localhost:9200"

# Sesuaikan dengan nama index yang dibuat
INDEX_NAME = "global_landslide_catalog"

HEADERS = {
    "Content-Type": "application/json"
}


# ==========================================
# FUNGSI PENGUJIAN
# ==========================================

def run_test(test_name, endpoint, query=None, method="GET"):

    print("\n" + "=" * 65)
    print(test_name)
    print("=" * 65)

    start_time = time.perf_counter()

    try:
        if method == "POST":
            response = requests.post(
                endpoint,
                headers=HEADERS,
                json=query,
                timeout=60
            )
        else:
            response = requests.get(
                endpoint,
                timeout=60
            )

        duration = (time.perf_counter() - start_time) * 1000

        print("HTTP Status :", response.status_code)
        print("Execution   :", round(duration, 2), "ms")

        try:
            result = response.json()
            print(json.dumps(
                result,
                indent=4,
                ensure_ascii=False
            ))
        except ValueError:
            print(response.text)

        return response.status_code, duration

    except requests.exceptions.RequestException as error:
        print("ERROR:", error)
        return None, None


# ==========================================
# 1. CLUSTER HEALTH
# ==========================================

def test_cluster_health():

    run_test(
        "1. CLUSTER HEALTH",
        f"{ES_HOST}/_cluster/health"
    )


# ==========================================
# 2. COUNT DOCUMENTS
# ==========================================

def test_count_documents():

    run_test(
        "2. COUNT DOCUMENTS",
        f"{ES_HOST}/{INDEX_NAME}/_count"
    )


# ==========================================
# 3. FULL TEXT SEARCH
# ==========================================

def test_full_text_search():

    query = {
        "query": {
            "multi_match": {
                "query": "landslide",
                "fields": [
                    "event_title",
                    "event_description",
                    "location_description",
                    "notes"
                ]
            }
        },
        "size": 5
    }

    run_test(
        "3. FULL TEXT SEARCH",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 4. FILTER NEGARA
# ==========================================

def test_filter_country():

    query = {
        "query": {
            "term": {
                "country_code": "CN"
            }
        },
        "size": 5
    }

    run_test(
        "4. FILTER BERDASARKAN NEGARA",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 5. FILTER TAHUN
# ==========================================

def test_filter_year():

    query = {
        "query": {
            "term": {
                "event_year": 2008
            }
        },
        "size": 5
    }

    run_test(
        "5. FILTER BERDASARKAN TAHUN",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 6. GEO-SPATIAL SEARCH
# ==========================================

def test_geo_spatial():

    query = {
        "query": {
            "geo_distance": {
                "distance": "100km",
                "geo_point": {
                    "lat": -6.2,
                    "lon": 106.8
                }
            }
        },
        "size": 5
    }

    run_test(
        "6. GEO-SPATIAL SEARCH",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 7. AGGREGATION NEGARA
# ==========================================

def test_aggregation_country():

    query = {
        "size": 0,
        "aggs": {
            "jumlah_per_negara": {
                "terms": {
                    "field": "country_name",
                    "size": 10
                }
            }
        }
    }

    run_test(
        "7. AGGREGATION BERDASARKAN NEGARA",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 8. AGGREGATION KATEGORI LONGSOR
# ==========================================

def test_aggregation_category():

    query = {
        "size": 0,
        "aggs": {
            "jumlah_per_kategori": {
                "terms": {
                    "field": "landslide_category",
                    "size": 10
                }
            }
        }
    }

    run_test(
        "8. AGGREGATION KATEGORI LONGSOR",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# 9. AGGREGATION TAHUN
# ==========================================

def test_aggregation_year():

    query = {
        "size": 0,
        "aggs": {
            "jumlah_per_tahun": {
                "terms": {
                    "field": "event_year",
                    "size": 15,
                    "order": {
                        "_key": "asc"
                    }
                }
            }
        }
    }

    run_test(
        "9. AGGREGATION BERDASARKAN TAHUN",
        f"{ES_HOST}/{INDEX_NAME}/_search",
        query,
        "POST"
    )


# ==========================================
# EKSEKUSI SELURUH PENGUJIAN
# ==========================================

if __name__ == "__main__":

    print("\n")
    print("#" * 65)
    print("       ELASTICSEARCH TESTING - NASA GLC DATASET")
    print("#" * 65)

    test_cluster_health()
    test_count_documents()
    test_full_text_search()
    test_filter_country()
    test_filter_year()
    test_geo_spatial()
    test_aggregation_country()
    test_aggregation_category()
    test_aggregation_year()

    print("\n")
    print("#" * 65)
    print("              ALL TESTS COMPLETED")
    print("#" * 65)

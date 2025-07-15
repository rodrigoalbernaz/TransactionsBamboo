import streamlit as st
from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta

# Configuración de conexión a Elasticsearch

es = Elasticsearch(
    st.secrets["ELASTIC_URL"],
    basic_auth=(st.secrets["ELASTIC_USER"], st.secrets["ELASTIC_PASS"]),
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
    },
    request_timeout=60,
    verify_certs=False
)


# --- Interfaz Streamlit ---
st.title("Buscar HttpRequest y HttpResponse por ID")

ambiente = st.selectbox("Ambiente", ["stage", "prod"])
tipo = st.selectbox("Tipo de operación", ["Payout", "Payin"])

# Payin: versión v1 o v3
version = None
if tipo == "Payin":
    version = st.radio("Versión de API", ["v3", "v1"], horizontal=True)

# Campo a buscar
if tipo == "Payout":
    field_name = "PayoutId"
elif tipo == "Payin":
    field_name = "TransactionId" if version == "v3" else "PurchaseId"

input_id = st.text_input(f"Ingresá el {field_name}")

if input_id:
    with st.spinner("Buscando LogHashKey..."):

        app_name = "Payout.API" if tipo == "Payout" else "ServiceFacade"
        indice = f"{ambiente}*"
        data_view_id = "79dff280-0c66-11ec-b6fc-490b1c72e533" if ambiente == "stage" else "ca9d9b00-d2db-11ec-9e8c-5965b351e5ed"

        # Buscar LogHashKey
        query_hash = {
            "query": {
                "match_phrase": {
                    "message": f'"{field_name}":{input_id}'
                }
            },
            "size": 1
        }

        try:
            res_hash = es.search(index=indice, body=query_hash)
        except Exception as e:
            st.error(f"Error consultando Elasticsearch: {e}")
            st.stop()

        if not res_hash["hits"]["hits"]:
            st.error(f"No se encontró LogHashKey para {field_name} {input_id}")
        else:
            source_hash = res_hash["hits"]["hits"][0]["_source"]
            log_hash = source_hash["fields"]["LogHashKey"]
            timestamp = source_hash["@timestamp"]
            st.success(f"LogHashKey encontrado: {log_hash}")

            # Armar link a Kibana con vista, data view y tiempo
            view_id = "e98f1800-f48c-11eb-a9c0-f5a5ca3d3cc2"
            kibana_base = "https://elastic-observability-bamboo.kb.us-east-1.aws.found.io/app/discover#/view"

            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            from_dt = (dt - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            to_dt = (dt + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            g_param = f"_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:'{from_dt}',to:'{to_dt}'))"

            a_param = (
                f"_a=(columns:!(fields.ApplicationName,fields.LogHashKey,message,level),"
                f"dataSource:(dataViewId:'{data_view_id}',type:dataView),"
                f"filters:!((meta:(index:'{data_view_id}',type:phrase,key:fields.LogHashKey,params:(query:'{log_hash}')),"
                f"query:(match_phrase:(fields.LogHashKey:'{log_hash}')))),"
                f"hideChart:!t,interval:auto,query:(language:kuery,query:''),"
                f"sort:!(!('@timestamp',desc)))"

            )

            kibana_url = f"{kibana_base}/{view_id}?{g_param}&{a_param}"

            st.markdown(f"[🔍 Ver logs en Kibana]({kibana_url})", unsafe_allow_html=True)
            st.text_input("URL completa de Kibana", kibana_url)

            # --- Buscar HttpRequest ---
            query_request = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"fields.LogHashKey": log_hash}},
                            {"match_phrase": {"message": "HttpRequest"}},
                            {"match": {"fields.ApplicationName": app_name}}
                        ]
                    }
                },
                "size": 1
            }

            try:
                res_req = es.search(index=indice, body=query_request)
            except Exception as e:
                st.error(f"Error al consultar HttpRequest: {e}")
                st.stop()

            if not res_req["hits"]["hits"]:
                st.warning("No se encontró HttpRequest relacionado.")
            else:
                msg = res_req["hits"]["hits"][0]["_source"]["message"]
                if "HttpRequest:" in msg:
                    msg = msg.split("HttpRequest:")[1].strip()

                try:
                    json_start = msg.index("{")
                    json_text = msg[json_start:]
                    open_braces = json_text.count("{")
                    close_braces = json_text.count("}")
                    if open_braces > close_braces:
                        json_text += "}" * (open_braces - close_braces)

                    parsed = json.loads(json_text)

                    if "body" in parsed:
                        with st.expander(f"Ver Request - body ({tipo})", expanded=False):
                            st.json(parsed["body"])
                            st.download_button(
                                label="Descargar body (JSON)",
                                data=json.dumps(parsed["body"], indent=2, ensure_ascii=False),
                                file_name=f"{ambiente}_{tipo.lower()}_body.json",
                                mime="application/json"
                            )

                    with st.expander(f"Ver Request completo ({tipo})", expanded=False):
                        st.json(parsed)
                        st.download_button(
                            label="Descargar request completo (JSON)",
                            data=json.dumps(parsed, indent=2, ensure_ascii=False),
                            file_name=f"{ambiente}_{tipo.lower()}_request.json",
                            mime="application/json"
                        )

                except Exception as e:
                    st.error(f"Error al procesar el JSON del request: {e}")
                    st.code(msg)

            # --- Buscar HttpResponse ---
            if tipo == "Payin" and version == "v1":
                query_response = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"fields.LogHashKey": log_hash}},
                                {"match_phrase": {"message": "\"body\":{\"Response\":{\"PurchaseId\":"}},
                                {"match": {"fields.ApplicationName": "ServiceFacade"}}
                            ]
                        }
                    },
                    "size": 1
                }
            else:
                query_response = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"fields.LogHashKey": log_hash}},
                                {"match_phrase": {"message": "HttpResponse"}},
                                {"match": {"fields.ApplicationName": app_name}}
                            ]
                        }
                    },
                    "size": 1
                }

            try:
                res_resp = es.search(index=indice, body=query_response)
                if res_resp["hits"]["hits"]:
                    msg_resp = res_resp["hits"]["hits"][0]["_source"]["message"]
                    if "HttpResponse:" in msg_resp:
                        msg_resp = msg_resp.split("HttpResponse:")[1].strip()

                    try:
                        start = msg_resp.index("{")
                        resp_json = msg_resp[start:]
                        open_b = resp_json.count("{")
                        close_b = resp_json.count("}")
                        if open_b > close_b:
                            resp_json += "}" * (open_b - close_b)

                        resp_parsed = json.loads(resp_json)

                        if "body" in resp_parsed:
                            with st.expander(f"Ver Response - body ({tipo})", expanded=False):
                                st.json(resp_parsed["body"])
                                st.download_button(
                                    label="Descargar response body (JSON)",
                                    data=json.dumps(resp_parsed["body"], indent=2, ensure_ascii=False),
                                    file_name=f"{ambiente}_{tipo.lower()}_response_body.json",
                                    mime="application/json"
                                )

                        with st.expander("Ver Response completo", expanded=False):
                            st.json(resp_parsed)
                            st.download_button(
                                label="Descargar response completo (JSON)",
                                data=json.dumps(resp_parsed, indent=2, ensure_ascii=False),
                                file_name=f"{ambiente}_{tipo.lower()}_response.json",
                                mime="application/json"
                            )

                    except Exception as e:
                        st.warning("No se pudo parsear el response:")
                        st.code(msg_resp)
                else:
                    st.warning("No se encontró HttpResponse relacionado.")

            except Exception as e:
                st.error(f"Error al consultar HttpResponse: {e}")

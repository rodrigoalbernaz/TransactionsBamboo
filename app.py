import streamlit as st
from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta

# Diccionario de errores de Bamboo
error_code_map = {
    "TK001": "Card number is incorrect. Ask your customer to check it and retry.",
    "TK002": "CVV number is incorrect. Ask your customer to verify and retry.",
    "TK003": "Expiration date is incorrect. Ask your customer to verify and retry.",
    "TK004": "Invalid session ID in token request. Regenerate and retry.",
    "TK005": "Email format is incorrect. Ask your customer to verify it.",
    "TK006": "One-time token expired or used. Regenerate and retry.",
    "TK007": "Invalid payment method info. Verify PaymentMediaId.",
    "TK008": "Issuer bank mismatch. Validate issuer bank.",
    "TK009": "Invalid token activation code. Contact Bamboo support.",
    "TK010": "Invalid Commerce Token. Regenerate and retry.",
    "TK011": "Invalid or missing customer. Review customer info.",
    "TK012": "Token activation error. Contact Bamboo support.",
    "TK013": "Registration error. Contact Bamboo support.",
    "TK014": "Payment method disabled. Contact Bamboo support.",
    "TK015": "Payment method not available for the Commerce. Check configuration.",
    "TK016": "Error registering payment method. Contact Bamboo support.",
    "TK017": "Invalid document. Check document according to country rules.",
    "TK018": "Invalid document type. Ensure it belongs to the selected country.",
    "TK019": "Invalid payment type. Ensure compatibility with country.",
    "TK020": "Invalid authentication token. Verify value.",
    "TK021": "Missing authentication token. Ensure it is sent.",
    "TK022": "Invalid token data. Contact Bamboo support.",
    "TK023": "Authentication already processed.",
    "TK024": "Authentication does not need 3DS.",
    "TK999": "Unknown tokenization error. Contact Bamboo support.",
    "TR001": "Communication error with acquiring service. Contact support.",
    "TR002": "Invalid transaction state. Example: Commit on rejected Purchase.",
    "TR003": "Merchant account issue with Acquirer.",
    "TR004": "Error sending transaction to Acquirer via Proxy.",
    "TR005": "Internal Acquirer error.",
    "TR006": "Duplicate order number at Acquirer.",
    "TR007": "Payment data errors: card number, CVV, expiration date.",
    "TR008": "Commit amount higher than authorized.",
    "TR009": "Unknown Acquirer error.",
    "TR010": "Invalid document number. Customer must verify it.",
    "TR011": "Blocked or lost card. Contact issuer.",
    "TR012": "Credit limit exceeded.",
    "TR013": "Transaction denied by Acquirer or Issuer.",
    "TR014": "Denied for possible fraud (Acquirer’s anti-fraud).",
    "TR015": "Manual review suggested (fraud suspicion).",
    "TR016": "Invalid or incomplete parameters sent to Acquirer.",
    "TR017": "Invalid transaction type.",
    "TR018": "Card registration denied by Acquirer.",
    "TR019": "Transaction rejected by Acquirer or processor.",
    "TR020": "Issuer requires verbal authorization.",
    "TR021": "Expired or mismatched expiration date.",
    "TR022": "CVV invalid according to Acquirer.",
    "TR023": "Inactive card or not authorized for this transaction.",
    "TR024": "Usage frequency or amount limit exceeded.",
    "TR025": "Invalid address data.",
    "TR026": "Insufficient funds.",
    "TR027": "Issuer requires manual authorization.",
    "TR028": "Paid amount doesn't match purchase amount.",
    "TR030": "Retry limit exceeded. Use another card.",
    "TR031": "Account closed. Contact issuer bank.",
    "TR032": "Declined. Contact card-issuing bank.",
    "TR033": "Installments not allowed for international cards.",
    "TR035": "Bank info missing. Cannot process refund.",
    "TR036": "Invalid bank data or mismatch in beneficiary.",
    "TR075": "3DSecure requires customer validation.",
    "TR076": "Payer authentication failed. Contact issuer.",
    "TR100": "Acquirer rejected for multiple reasons.",
    "TR101": "Refund cannot be processed by Acquirer.",
    "TR301": "Rejected by Bamboo’s anti-fraud system.",
    "TR302": "Invalid anti-fraud parameters.",
    "TR996": "Internal processing error. Try again.",
    "TR997": "Execution error during process.",
    "TR999": "Undetermined error. Contact support.",
     # Purchase errors (PR)
    "PR001": "The informed token is invalid, expired or does not correspond to the commerce.",
    "PR002": "The order number is invalid.",
    "PR003": "The provided amount is invalid.",
    "PR004": "Invalid Currency parameter for the Purchase.",
    "PR005": "The invoice number is invalid.",
    "PR006": "Invalid Purchase Id for the Purchase.",
    "PR007": "Invalid TransactionID for the Purchase.",
    "PR008": "The requested purchase cannot be found.",
    "PR009": "The current purchase state does not allow the requested operation.",
    "PR010": "The TaxableAmount field is required.",
    "PR011": "The Invoice field is required.",
    "PR012": "Capture of the card verification code is required.",
    "PR013": "Invalid installments for the card.",
    "PR014": "Invalid parameter length description.",
    "PR015": "UserAgent parameter is empty.",
    "PR016": "CustomerIP parameter is empty.",
    "PR017": "TaxableAmount cannot be greater than the total amount.",
    "PR018": "Must enter From and To dates to filter.",
    "PR019": "Search period exceeds maximum allowed.",
    "PR020": "Invalid registered document.",
    "PR021": "Partial refunds are not allowed.",
    "PR034": "Invalid TargetCountryISO value.",
    # Payout-specific errors
    "000": "Invalid Code.",
    "200": "Success.",
    "400": "Bad Request.",
    "401": "Unauthorized.",
    "409": "Conflict.",
    "601": "Invalid Destination Country.",
    "602": "Invalid Origin currency.",
    "603": "Invalid amount.",
    "604": "Invalid ISO code for destination currency.",
    "605": "Merchant account not found.",
    "606": "Merchant account not enabled.",
    "607": "Merchant account has an invalid business model.",
    "699": "Generic error for Purchase Preview.",
    "701": "Insufficient balance.",
    "702": "Declined by compliance.",
    "703": "General Error Balance.",
    "704": "Minimum payout amount is invalid.",
    "705": "Invalid origin currency ISO code.",
    "706": "The account was not found.",
    "707": "Account not enabled.",
    "708": "Invalid business model.",
    "709": "General error when obtaining business information.",
    "710": "Bank entered not valid.",
    "711": "Account entered not valid.",
    "712": "Data request expired.",
    "713": "General error when requesting data.",
    "812": "Declined by validation for document.",
    "813": "Declined by validation for account.",
    "814": "Declined by validation for country.",
    "816": "Reference ID already used.",
    "817": "Destination currency Unsupported.",
    "901": "Bank account is closed.",
    "902": "Invalid bank account.",
    "903": "Invalid bank account type.",
    "904": "Invalid bank branch.",
    "905": "Monthly limit exceeded for user.",
    "906": "Rejected by merchant’s request.",
    "907": "The bank account is unable to receive transfers.",
    "908": "Invalid beneficiary document.",
    "909": "Beneficiary name doesn’t match bank details.",
    "910": "PIX key invalid.",
    "911": "Invalid state change requested.",
    "912": "Insufficient Balance.",
    "913": "Invalid process date.",
    "914": "Insufficient Balance in integration.",
    "915": "General error in integration.",
    "916": "Bank reject.",
    "921": "Invalid wallet.",
    "999": "Error."
}

# Configuración de conexión a Elasticsearch

es = Elasticsearch(
    "https://elastic-observability-bamboo.es.us-east-1.aws.found.io:9243",
    basic_auth=("ralbernaz", "b5$6g&C7p4R-9cC"),
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
    },
    request_timeout=90,
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
                        # Mostrar código de error, descripción y status
                        error_code = None
                        status = None
                        try:
                            if tipo == "Payin":
                                if version == "v1":
                                    transaction = resp_parsed.get("body", {}).get("Response", {}).get("Transaction", {})
                                    error_code = transaction.get("ErrorCode")
                                    status = transaction.get("Status")
                                elif version == "v3":
                                    error_code = resp_parsed.get("body", {}).get("ErrorCode")
                                    status = resp_parsed.get("body", {}).get("Status")
                            elif tipo == "Payout":
                                status = resp_parsed.get("body", {}).get("StatusDescription")
                                errors = resp_parsed.get("body", {}).get("errors", [])
                                if errors and isinstance(errors, list):
                                    error_code = str(errors[0].get("Code"))
                        except Exception as e:
                            st.warning(f"No se pudo obtener datos de ErrorCode o Status: {e}")

                        if status:
                            st.markdown(f"**📋 Estado:** `{status}`")
                        if error_code:
                            error_description = error_code_map.get(error_code, "Código no reconocido. Contactar a soporte.")
                            st.markdown(f"**🛑 Código de error:** `{error_code}`")
                            st.markdown(f"**📘 Descripción:** {error_description}")
                        else:
                            st.success("✅ Transacción aprobada o sin código de error.")

                    except Exception as e:
                        st.warning("No se pudo parsear el response:")
                        st.code(msg_resp)
                else:
                    st.warning("No se encontró HttpResponse relacionado.")
            except Exception as e:
                st.error(f"Error consultando Elasticsearch para HttpResponse: {e}")
                st.error(f"Error al consultar HttpResponse: {e}")

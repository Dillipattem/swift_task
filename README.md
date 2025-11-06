# Transit Performance Analyzer

The Transit Performance Analyzer is a logistics insights tool that processes FedEx-style shipment tracking JSON and converts it into clean, analysis-ready datasets. It normalizes timestamps to IST, standardizes facility event logs, and computes key shipment-level and network-level transit performance metrics. The output can be used for reporting, SLA monitoring, operational decision-making, and network optimization.

---

## 🚚 Key Capabilities

- Parses nested shipment and scan event JSON files
- Converts and normalizes timestamps to **IST (UTC+05:30)**
- Flattens shipments and events into clean tabular datasets
- Computes logistics KPIs:
  - **Total Transit Time (Hours)**
  - **Unique Facility Touches**
  - **Inter-Facility Travel Time**
  - **OFD (Out for Delivery) Attempts**
  - **First Attempt Delivery Success**
- Generates:
  - **Shipment-level Detailed CSV**
  - **Overall Summary Metrics File**
  - **Service-Level Summary Report**

---

## 🧱 Tech Stack

| Component | Usage |
|----------|--------|
| **Python** | Core processing & metric calculations |
| **Pandas** | Data wrangling, transformation & analysis |
| **Jupyter Notebook** | Development, testing & exploration |

---

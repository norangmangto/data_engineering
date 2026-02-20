# CMS Medicare Claims (DE-SynPUF) Schema Documentation

## Overview

The CMS Medicare Claims dimensional model is designed to handle the complexity of healthcare insurance data. It uses a **Star Schema** with multiple fact tables at different grains and **Bridge Tables** for many-to-many relationships.

## Key Design Decisions

### 1. Multi-Grain Fact Tables

Unlike a standard retail model where everything might fit into a single `fact_sales` table, healthcare claims happen at different levels:
- **Inpatient**: One stay (admission to discharge) is the grain.
- **Outpatient**: One visit is the grain.
- **Carrier**: Individual physician/supplier services.
- **PDE**: Individual drug prescription fills.
- **Periodic Snapshot**: Annual summary per beneficiary.

Modeling these as separate fact tables prevents row explosion and allows joining to conformed dimensions (Date, Beneficiary, Provider) consistently.

### 2. Bridge Tables (Many-to-Many)

Claims often have multiple:
- **Diagnoses**: A single stay might have 10+ ICD-9 codes.
- **Procedures**: Multiple ICD-9/HCPCS codes per visit.
- **Providers**: Attending, Operating, and Other physicians.

Using a **Bridge Table** allows for granular analysis (e.g., "Find all claims for patients with Chronic Kidney Disease (ICD-9 585.x)") without duplicating measurements in the fact tables.

### 3. Conformed Dimensions

Standardized dimensions allow for **cross-drill** analysis:
- `dim_beneficiary`: Demographics and chronic conditions.
- `dim_provider`: Deduped NPIs across all settings (IP, OP, Carrier).
- `dim_diagnosis` / `dim_procedure`: Lookup tables for medical codes.
- `dim_date`: Unified date spine for longitudinal analysis.

## Data Quality & Testing

- **Unique/Not-Null**: Enforced on all surrogate and natural keys.
- **Relationships**: Validated between fact/bridge tables and dimensions.
- **Synthetic Limitations**: Note that NPI names and diagnosis descriptions are placeholder synthetic values in this version.

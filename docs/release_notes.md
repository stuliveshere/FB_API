# Facebook Ad Library API — Release Notes

Source: https://www.facebook.com/ads/library/api/releasenotes

---

## 17 Aug 2023

**Added**
- Search parameter: `unmask_removed_content`
- Search result fields: `age_country_gender_reach_breakdown`, `beneficiary_payers`, `eu_total_reach`, `target_ages`, `target_gender`, `target_locations`

**Updated**
- `ad_type` parameter now accepts: `ALL`, `CREDIT_ADS`, `EMPLOYMENT_ADS`, `HOUSING_ADS`, `POLITICAL_AND_ISSUE_ADS`

---

## 16 Nov 2021

**Added**
- Search parameter: `search_type`

---

## 11 Nov 2021

**Deprecated** *(after Graph API v13.0)*
- `potential_reach` → use `estimated_audience_size`
- `potential_reach_min` / `potential_reach_max` → use `estimated_audience_size_min` / `estimated_audience_size_max`

---

## 30 Sep 2021

**Deprecated** *(after Graph API v13.0)*
- `funding_entity` → use `bylines`

---

## 30 Jul 2021

**Added**
- `media_type` parameter now accepts: `MEME`

---

## 30 Jun 2021

**Added**
- Search parameter: `languages`

**Updated**
- `ad_active_status` default changed from `ACTIVE` to `ALL`

**Deprecated** *(after Graph API v13.0)*
- `region_distribution` → use `delivery_by_region`
- `ad_creative_body` → use `ad_creative_bodies`
- `ad_creative_link_caption` → use `ad_creative_link_captions`
- `ad_creative_link_description` → use `ad_creative_link_descriptions`
- `ad_creative_link_title` → use `ad_creative_link_titles`

---

## 23 Apr 2021

**Updated**
- Users now require a role (or assigned role by app owner) to make queries

---

## 5 Apr 2021

**Added**
- Search parameter: `media_type`

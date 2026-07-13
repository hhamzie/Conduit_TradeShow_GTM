#!/usr/bin/env python3
"""Build and deploy the Cultivate Airtable replacement loop to n8n."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / "n8n" / "cultivate_airtable_replacement_loop.json"

WORKFLOW_NAME = "Cultivate Airtable Replacement Loop - Wiza Skipped"
N8N_BASE_ENV = "N8N_BASE_URL"
N8N_API_ENV = "N8N_API_KEY"

AIRTABLE_BASE_ID = "appfBCKnwzWr26p8R"
AIRTABLE_TABLES = {
    "leads": "tblhzByGqquL0czNF",
    "sales": "tblh8mxFnH3xYuDkK",
    "ops": "tblwk9D9Ve1RxwfNa",
    "cs": "tblRtquRALrH3ZXEk",
}
APIFY_LINKEDIN_ACTOR_ID = "VMwB47uSx3g2wCcBK"
PIPEDRIVE_TRADESHOW_FIELD_KEY = "dfcc62a9104fce98ed1ea6566a9bd82d002a6986"
PIPEDRIVE_INDUSTRY_FIELD_KEY = "6d24142b70f5c8b1b140c9b278bb7c8141fcb8bc"
PIPEDRIVE_PERSON_LINKEDIN_PROFILE_FIELD_KEY = "57139293fba428fc3ffebc689b0fecc628569aa0"
PIPEDRIVE_PERSON_LINKEDIN_URL_FIELD_KEY = "2e0318b3854d2c3996417e4b74e8ba0ea1f80813"
PIPEDRIVE_DYNAMIC_EVENT_INDUSTRY_OPTION_ID = 277


REQUIRED_CREDENTIALS = {
    "airtable": "Conduit Airtable Bearer",
    "openai": "Conduit OpenAI Bearer Active",
    "leadmagic": "Conduit LeadMagic X-API-Key",
    "enrichley": "Conduit Enrichley X-Api-Key",
    "icypeas": "Conduit Icypeas Authorization",
    "prospeo": "Conduit Prospeo X-KEY",
    "findymail": "Conduit Findymail Bearer",
    "smartlead": "Conduit Smartlead Query api_key",
    "apify": "Conduit Apify Bearer",
    "pipedrive": "Conduit Pipedrive x-api-token",
}


NORMALIZE_INCOMING_LEADS_JS = r"""
function asArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function unwrapPayload(input) {
  const raw = input || {};
  if (raw.body && typeof raw.body === 'object') return raw.body;
  return raw;
}

function firstValue(row, names) {
  for (const name of names) {
    const value = row?.[name];
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      return String(value).trim();
    }
  }
  return '';
}

function domainFromUrl(value) {
  if (!value) return '';
  try {
    const raw = String(value).trim();
    const url = raw.match(/^https?:\/\//i) ? raw : `https://${raw}`;
    return new URL(url).hostname.replace(/^www\./i, '').toLowerCase();
  } catch (error) {
    return '';
  }
}

function titleCase(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b[a-z]/g, (letter) => letter.toUpperCase())
    .replace(/\b(And|Of|The|For|In)\b/g, (word) => word.toLowerCase())
    .trim();
}

function normalizeCompanyName(value) {
  return titleCase(
    String(value || '')
      .replace(/\b(incorporated|inc\.?|llc|l\.l\.c\.|ltd\.?|co\.?|corp\.?|corporation|company)\b/gi, '')
      .replace(/[,.]+$/g, '')
      .replace(/\s+/g, ' ')
      .trim()
  );
}

function toBoolean(value) {
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(value || '').trim().toLowerCase());
}

function isoDate(value) {
  const raw = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return '';
  const parsed = new Date(`${raw}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString().slice(0, 10);
}

const output = [];
for (const item of items) {
  const payload = unwrapPayload(item.json);
  const rows = [
    ...asArray(payload.rows),
    ...asArray(payload.leads),
    ...asArray(payload.records),
  ];
  const sourceRows = rows.length ? rows : asArray(payload);

  sourceRows.forEach((row, index) => {
    const fields = row.fields && typeof row.fields === 'object' ? row.fields : row;
    const brandName = firstValue(fields, ['brand_name', 'Brand Name', 'company_name', 'Company Name', 'company', 'name']);
    if (!brandName) return;

    const exhibitorUrl = firstValue(fields, ['exhibitor_url', 'Exhibitor URL', 'source_url', 'Source URL']);
    const website = firstValue(fields, ['website', 'Website', 'website_url', 'Website URL']);
    const suppliedDomain = firstValue(fields, ['Official Company Domain (2)', 'Official Company Domain', 'domain', 'Domain']);
    const domain = domainFromUrl(suppliedDomain || website || exhibitorUrl);
    const sourceRecordId = row.id || row.recordId || fields.record_id || fields.source_row_id || `incoming-${Date.now()}-${index + 1}`;
    const attachedContacts = Array.isArray(fields.scraped_contacts) ? fields.scraped_contacts : [];

    output.push({
      json: {
        sourceRecordId,
        sourceRowIndex: Number.isFinite(Number(fields.sourceRowIndex ?? fields.source_row_index ?? row.sourceRowIndex ?? row.source_row_index))
          ? Number(fields.sourceRowIndex ?? fields.source_row_index ?? row.sourceRowIndex ?? row.source_row_index)
          : index,
        rawLead: fields,
        exhibitor_url: exhibitorUrl,
        brand_name: brandName,
        shown_by: firstValue(fields, ['shown_by', 'Shown By']),
        showrooms: firstValue(fields, ['showrooms', 'Showrooms']),
        website,
        fax: firstValue(fields, ['fax', 'Fax']),
        phone: firstValue(fields, ['general_contact_phone', 'General Contact Phone', 'phone', 'Phone']),
        company_email: firstValue(fields, ['general_contact_email', 'General Contact Email', 'company_email', 'Company Email']),
        showroom_contact: firstValue(fields, ['showroom_contact', 'Showroom Contact']),
        scrapedContact: {
          fullName: firstValue(fields, ['contact_name', 'Contact Name', 'person_name', 'Person Name']),
          jobTitle: firstValue(fields, ['contact_title', 'Contact Title', 'job_title', 'Job Title']),
          email: firstValue(fields, ['contact_email', 'Contact Email']),
          phone: firstValue(fields, ['contact_phone', 'Contact Phone']),
          sourceUrl: firstValue(fields, ['contact_source_url', 'Contact Source URL', 'source_url', 'Source URL']),
        },
        scrapedContacts: attachedContacts.map((contact) => ({
          fullName: firstValue(contact, ['person_name', 'Person Name', 'contact_name', 'Contact Name']),
          jobTitle: firstValue(contact, ['job_title', 'Job Title', 'contact_title', 'Contact Title']),
          email: firstValue(contact, ['email', 'contact_email', 'Contact Email']),
          phone: firstValue(contact, ['phone', 'contact_phone', 'Contact Phone']),
          sourceUrl: firstValue(contact, ['source_url', 'Source URL']) || exhibitorUrl,
          contactSourceType: 'scraped_trade_show_contact',
        })).filter((contact) => contact.fullName),
        address: firstValue(fields, ['address', 'Address']),
        instagram: firstValue(fields, ['instagram', 'Instagram']),
        contact_info: firstValue(fields, ['contact_info', 'Contact Info']),
        error: firstValue(fields, ['error', 'Error']),
        conference: firstValue(fields, ['Conference', 'conference', 'show_name', 'Show Name']) || payload.show?.name || '',
        booth_number: firstValue(fields, ['booth_number', 'Booth Number', 'booth']),
        suppliedDomain: domain,
        normalizedName: firstValue(fields, ['Normalized Name', 'Normalized Company Name']) || normalizeCompanyName(brandName),
        smartleadCampaignId: String(
          payload.smartleadCampaignId
          || fields.smartleadCampaignId
          || fields['Smartlead Campaign ID']
          || ''
        ).trim(),
        cadenceEnrollmentDate: isoDate(
          payload.cadenceEnrollmentDate
          || payload.campaignStartDate
          || fields.cadenceEnrollmentDate
          || fields['Cadence Enrollment Date']
        ) || new Date().toISOString().slice(0, 10),
        enableSmartlead: toBoolean(payload.enableSmartlead ?? fields.enableSmartlead ?? fields['Enable Smartlead']),
      }
    });
  });
}

return output;
"""


BUILD_RESEARCH_REQUEST_JS = r"""
function compact(value) {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined && item !== null && item !== ''));
}

const row = item.json;
const model = (typeof $vars !== 'undefined' && ($vars.CONDUIT_OPENAI_RESEARCH_MODEL || $vars.CONDUIT_OPENAI_MODEL)) || 'gpt-4o';
const showName = row.conference || row.showName || 'trade show';
const userPrompt = `
You are doing strict pre-show exhibitor prospecting for ${showName}.
Return only one valid JSON object.

Company/exhibitor input:
${JSON.stringify(compact({
  brand_name: row.brand_name,
  exhibitor_url: row.exhibitor_url,
  website: row.website,
  showroom_contact: row.showroom_contact,
  company_email: row.company_email,
  supplied_domain: row.suppliedDomain,
  conference: row.conference,
  booth_number: row.booth_number
}), null, 2)}

Tasks:
1. Find the official root company domain. Return root domain only, no protocol and no www.
2. Normalize the company name.
3. Find current leaders for exactly these personas. For each persona, search broadly enough to return up to three candidates before selecting the best one:
   - sales: current sales/revenue/commercial leader. Title must directly contain sales, revenue, commercial, growth, business development, partnerships, account executive, account management, CRO, or chief revenue. Do not use founder/CEO/president unless the title also clearly says sales/revenue/commercial.
   - cs: current customer support/customer success/customer service/client success/client services/customer advocacy leader. Title must directly contain customer support, customer service, customer success, client services, support, service, or customer care. Reject sales, operations, HR, founder, CEO, president, office/admin, generic contact, and directory-only matches unless the title clearly has one of those customer/support phrases.
   - ops: current operations/supply-chain/logistics/manufacturing/production leader. Prefer COO, operations VP/director/head/manager, supply chain, logistics, production, procurement, fulfillment, warehouse, or manufacturing. Do not use sales/support/CEO/president-only matches.
4. For each persona, return selected fullName, jobTitle, sourceUrl, confidence, linkedinUrl, linkedinActive, linkedinActivitySummary, and candidates.
5. confidence must be High, Medium, Low, or Not Found.
6. Use High only when the source clearly supports the person's exact full name, current company, and current persona-specific title.
7. Do not use Wiza, Apollo, ZoomInfo, RocketReach, SignalHire, Lusha, ContactOut, or similar people-directory/sales-intelligence pages as sourceUrl. Prefer the official company site, company press/news pages, a real LinkedIn public profile, or a credible industry/news page.
8. Do not invent LinkedIn URLs. Return linkedinUrl only when you found an exact public LinkedIn /in/ profile that visibly matches the full name and current company/title context. Otherwise return an empty string.
9. linkedinActive must be YES only when you found recent visible LinkedIn posts, comments, or reactions for that exact public profile. Otherwise use NO.
10. If the best match has only one name, weak source support, a mismatched title, or a directory-only source, return confidence "Not Found" for that persona.
11. Prefer current employee evidence over stale award pages, old conference bios, reseller pages, or directory snippets. If evidence is old or ambiguous, downgrade confidence.
12. Put concise evidence in each candidate's evidence field. Use rejectionReason when a candidate looks close but should not be selected.
13. Before returning Not Found for a persona, explicitly try exact searches using the company name plus persona/title words. Examples: "${row.brand_name} director of sales", "${row.brand_name} vice president sales", "${row.brand_name} customer service manager", "${row.brand_name} customer support manager", "${row.brand_name} customer success", "${row.brand_name} operations manager", "${row.brand_name} manufacturing manager".
14. For furniture, outdoor living, design, hospitality, and trade-show manufacturers, credible trade publications are acceptable evidence. Examples include Furniture Today, Casual News Now, Home Accents Today, ICFA member news, Business of Home, and similar industry publications.
15. Do not return all personas as Not Found just because the official company website has no team page. Use credible trade/news evidence when it names the person, company, and matching role.
16. If the evidence connects the person to a related distributor, parent, subsidiary, or brand but not clearly to this exhibitor, downgrade to Medium or Not Found and explain the relationship in evidence.
17. Prefer one very clean persona match over filling all personas. It is better to return Not Found than a plausible but weak contact.

JSON shape:
{
  "officialCompanyDomain": "",
  "normalizedCompanyName": "",
  "sales": {"firstName":"","fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","linkedinActive":"NO","linkedinActivitySummary":"","evidence":"","candidates":[{"fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","evidence":"","rejectionReason":""}]},
  "cs": {"firstName":"","fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","linkedinActive":"NO","linkedinActivitySummary":"","evidence":"","candidates":[{"fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","evidence":"","rejectionReason":""}]},
  "ops": {"firstName":"","fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","linkedinActive":"NO","linkedinActivitySummary":"","evidence":"","candidates":[{"fullName":"","jobTitle":"","sourceUrl":"","confidence":"","linkedinUrl":"","evidence":"","rejectionReason":""}]}
}`;

return {
  json: {
    ...row,
    openaiResearchRequest: {
      model,
      tools: [{ type: 'web_search' }],
      input: userPrompt
    }
  }
};
"""


PARSE_RESEARCH_RESULT_JS = r"""
function stableStringify(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch (error) {
    return String(value ?? '');
  }
}

function outputText(response) {
  if (!response) return '';
  if (typeof response.output_text === 'string') return response.output_text;
  if (Array.isArray(response.output)) {
    return response.output.flatMap((entry) => {
      if (Array.isArray(entry.content)) {
        return entry.content.map((part) => part.text || part.output_text || '').filter(Boolean);
      }
      return [entry.text || ''];
    }).filter(Boolean).join('\n');
  }
  return response.text || response.message || '';
}

function collectAnnotations(response) {
  const annotations = [];
  if (!response || !Array.isArray(response.output)) return annotations;
  for (const entry of response.output) {
    for (const part of entry.content || []) {
      for (const annotation of part.annotations || []) {
        if (annotation?.url) {
          annotations.push({
            url: String(annotation.url || ''),
            title: String(annotation.title || annotation.text || ''),
          });
        }
      }
    }
  }
  return annotations;
}

function parseJsonFromText(text) {
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (error) {
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return {};
    try {
      return JSON.parse(match[0]);
    } catch (nestedError) {
      return {};
    }
  }
}

function cleanDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = raw.match(/^https?:\/\//i) ? raw : `https://${raw}`;
    return new URL(url).hostname.replace(/^www\./i, '').toLowerCase();
  } catch (error) {
    return raw.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split('/')[0].toLowerCase();
  }
}

function firstName(fullName, fallback) {
  return String(fallback || fullName || '').replace(/^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+/i, '').split(/\s+/).filter(Boolean)[0] || '';
}

function sourceHost(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (!/^(https?:\/\/)?[a-z0-9.-]+\.[a-z]{2,}/i.test(raw)) return '';
  try {
    const url = raw.match(/^https?:\/\//i) ? raw : `https://${raw}`;
    return new URL(url).hostname.replace(/^www\./i, '').toLowerCase();
  } catch (error) {
    return raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split('/')[0]
      .split('?')[0]
      .toLowerCase();
  }
}

let webAnnotations = [];

function normalizeForMatch(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function cleanSourceUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (sourceHost(raw)) return raw;
  const needle = normalizeForMatch(raw);
  if (!needle) return '';
  const scored = webAnnotations
    .map((annotation) => {
      const title = normalizeForMatch(annotation.title);
      const overlap = needle.split(' ').filter((word) => word.length > 3 && title.includes(word)).length;
      return { annotation, overlap };
    })
    .sort((a, b) => b.overlap - a.overlap);
  if (scored[0]?.overlap >= 3) return scored[0].annotation.url;
  return '';
}

function isWeakDirectorySource(value) {
  const host = sourceHost(value);
  if (!host) return true;
  return [
    'wiza.co',
    'apollo.io',
    'zoominfo.com',
    'rocketreach.co',
    'signalhire.com',
    'lusha.com',
    'contactout.com',
    'adapt.io',
    'lead411.com',
    'leadIQ.com'.toLowerCase(),
    'peoplelooker.com',
    'beenverified.com',
    'visualvisitor.com',
    'growjo.com',
    'bizprofile.net',
  ].some((domain) => host === domain || host.endsWith(`.${domain}`));
}

function hasFullName(value) {
  return String(value || '').trim().split(/\s+/).filter(Boolean).length >= 2;
}

function hasPersonaTitle(persona, title) {
  const normalized = String(title || '').toLowerCase();
  if (!normalized || normalized.includes('not found')) return false;
  if (persona === 'sales') {
    return /\b(sales|revenue|commercial|growth|business development|partnerships?|account executive|account management|cro|chief revenue)\b/i.test(normalized);
  }
  if (persona === 'ops') {
    return /\b(operations?|ops|coo|chief operating|supply chain|logistics?|manufacturing|production|procurement|fulfillment|warehouse)\b/i.test(normalized);
  }
  if (/\b(sales|revenue|business development|operations?|ops|supply chain|logistics?|hr|human resources|finance|accounting|admin|administrator|office)\b/i.test(normalized)) {
    return /\b(customer success|customer support|customer service|client success|client services?|customer advocacy|customer care|support manager|support director|service manager|service director)\b/i.test(normalized);
  }
  return /\b(customer success|customer support|customer service|client success|client services?|customer advocacy|customer care|support manager|support director|service manager|service director|head of support|head of service)\b/i.test(normalized);
}

function cleanLinkedInUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = raw.match(/^https?:\/\//i) ? raw : `https://${raw}`;
    const parsed = new URL(url);
    const host = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    if (!(host === 'linkedin.com' || host.endsWith('.linkedin.com')) || !parsed.pathname.toLowerCase().startsWith('/in/')) return '';
    const slug = parsed.pathname.split('/').filter(Boolean)[1] || '';
    if (!slug || /12345678|example|placeholder|your-name|first-last/i.test(slug)) return '';
    return `https://www.linkedin.com${parsed.pathname.replace(/\/+$/, '')}`;
  } catch (error) {
    const match = raw.match(/https?:\/\/(?:[\w-]+\.)?linkedin\.com\/in\/[^\s"')?#]+/i);
    if (!match) return '';
    const slug = match[0].split('/in/')[1]?.replace(/\/+$/, '') || '';
    if (!slug || /12345678|example|placeholder|your-name|first-last/i.test(slug)) return '';
    return `https://www.linkedin.com/in/${slug}`;
  }
}

function person(value, includeCandidates = false) {
  const source = value && typeof value === 'object' ? value : {};
  const fullName = String(source.fullName || source.name || '').trim();
  const output = {
    firstName: firstName(fullName, source.firstName),
    fullName,
    jobTitle: String(source.jobTitle || source.title || '').trim(),
    sourceUrl: cleanSourceUrl(source.sourceUrl || source.url || ''),
    confidence: String(source.confidence || 'Not Found').trim(),
    linkedinUrl: cleanLinkedInUrl(source.linkedinUrl || source.personLinkedin || source.linkedin || ''),
    linkedinActive: String(source.linkedinActive || source.isActive || 'NO').trim().toUpperCase() === 'YES' ? 'YES' : 'NO',
    linkedinActivitySummary: String(source.linkedinActivitySummary || source.activitySummary || source.summary || '').trim(),
    evidence: String(source.evidence || source.reason || '').trim(),
    rejectionReason: String(source.rejectionReason || '').trim(),
  };
  if (includeCandidates && Array.isArray(source.candidates)) {
    output.candidates = source.candidates.map((candidate) => person(candidate)).filter((candidate) => candidate.fullName || candidate.jobTitle || candidate.sourceUrl);
  }
  return output;
}

function confidenceRank(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized.includes('high')) return 10;
  if (normalized.includes('medium')) return 5;
  if (normalized.includes('low')) return 1;
  return 0;
}

function sourceScore(value, officialDomain) {
  const host = sourceHost(value);
  if (!host || isWeakDirectorySource(value)) return -100;
  if (host === 'linkedin.com') return 18;
  if (officialDomain && (host === officialDomain || host.endsWith(`.${officialDomain}`))) return 25;
  if (/\b(furnituretoday|businesswire|prnewswire|globenewswire|news|magazine|journal|market|homeaccentstoday|casualnewsnow|icfanet|businessofhome)\b/i.test(host)) return 12;
  if (['theorg.com', 'ahfa.us', 'hotelspeconline.com'].some((domain) => host === domain || host.endsWith(`.${domain}`))) return 8;
  return -100;
}

function candidateScore(contact, persona, officialDomain) {
  if (!hasFullName(contact.fullName) || !hasPersonaTitle(persona, contact.jobTitle)) return -100;
  const source = sourceScore(contact.sourceUrl, officialDomain);
  if (source < 0) return -100;
  let score = 0;
  score += 12;
  score += 12;
  score += source;
  score += confidenceRank(contact.confidence);
  if (contact.linkedinUrl) score += 3;
  if (contact.evidence) score += 2;
  if (/\b(vp|vice president|director|head|chief|cro|coo|manager|lead)\b/i.test(contact.jobTitle)) score += 3;
  if (contact.rejectionReason) score -= 20;
  return score;
}

function normalizePersonForPersona(rawContact, persona, officialDomain) {
  const selected = person(rawContact, true);
  const extraCandidates = [parsed?.[`${persona}Candidates`], parsed?.[`${persona}_candidates`]]
    .filter(Array.isArray)
    .flat()
    .map((candidate) => person(candidate));
  const candidates = [
    selected,
    ...(selected.candidates || []),
    ...extraCandidates,
  ].filter((candidate) => candidate.fullName || candidate.jobTitle || candidate.sourceUrl);
  const ranked = candidates
    .map((candidate) => ({ candidate, score: candidateScore(candidate, persona, officialDomain) }))
    .sort((a, b) => b.score - a.score);
  const best = ranked[0]?.score >= 30 ? ranked[0].candidate : selected;
  const output = { ...best };
  output.candidates = ranked.map(({ candidate, score }) => ({ ...candidate, score })).slice(0, 3);
  const strongEnough = (
    hasFullName(output.fullName) &&
    hasPersonaTitle(persona, output.jobTitle) &&
    !isWeakDirectorySource(output.sourceUrl) &&
    candidateScore(output, persona, officialDomain) >= 30
  );
  if (!strongEnough) {
    output.confidence = 'Not Found';
    output.linkedinActive = 'NO';
    if (!hasFullName(output.fullName) || !hasPersonaTitle(persona, output.jobTitle)) {
      output.fullName = hasFullName(output.fullName) ? output.fullName : 'Not Found';
      output.jobTitle = hasPersonaTitle(persona, output.jobTitle) ? output.jobTitle : 'Not Found';
    }
  } else if (candidateScore(output, persona, officialDomain) >= 34) {
    output.confidence = 'High';
  } else if (!['high', 'medium', 'low'].includes(String(output.confidence || '').toLowerCase())) {
    output.confidence = 'Medium';
  }
  if (!output.linkedinUrl) {
    output.linkedinActive = 'NO';
    output.linkedinActivitySummary = '';
  }
  return output;
}

const original = $('Build Lead Research Request').item.json;
const response = item.json || {};
webAnnotations = collectAnnotations(response);
const researchError = response.error
  ? String(response.error.message || response.error.code || 'OpenAI research error').slice(0, 1000)
  : '';
const text = outputText(response);
const parsed = parseJsonFromText(text);
const domain = cleanDomain(parsed.officialCompanyDomain || original.suppliedDomain || original.website || original.exhibitor_url);
const sales = normalizePersonForPersona(parsed.sales, 'sales', domain);
const cs = normalizePersonForPersona(parsed.cs, 'cs', domain);
const ops = normalizePersonForPersona(parsed.ops, 'ops', domain);
const normalizedName = String(parsed.normalizedCompanyName || original.normalizedName || original.brand_name || '').trim();

function isQualified(contact) {
  return Boolean(
    domain &&
    contact.fullName &&
    contact.jobTitle &&
    !contact.fullName.toLowerCase().includes('not found') &&
    !contact.jobTitle.toLowerCase().includes('not found') &&
    contact.confidence.toLowerCase().includes('high')
  );
}

const leadFields = {
  exhibitor_url: original.exhibitor_url,
  brand_name: original.brand_name,
  shown_by: original.shown_by,
  showrooms: original.showrooms,
  website: original.website,
  fax: original.fax,
  phone: original.phone,
  company_email: original.company_email,
  showroom_contact: original.showroom_contact,
  address: original.address,
  instagram: original.instagram,
  contact_info: original.contact_info,
  error: original.error || researchError,
  'Official Company Domain': stableStringify({ response: parsed.officialCompanyDomain || domain, raw: parsed, error: researchError }),
  'Official Company Domain (2)': domain,
  'Sales Leader Info': stableStringify(sales),
  'Sales Full Name': sales.fullName,
  'Sales Job Title': sales.jobTitle,
  'Sales Confidence': sales.confidence,
  'Sales Source URL': sales.sourceUrl,
  'Top Support Decision Maker': stableStringify(cs),
  'CS Full Name': cs.fullName,
  'CS Job Title': cs.jobTitle,
  'CS Confidence': cs.confidence,
  'CS Source URL': cs.sourceUrl,
  'VP Operations Contact': stableStringify(ops),
  'Ops Full Name': ops.fullName,
  'Ops Job Title': ops.jobTitle,
  'Ops Source Url': ops.sourceUrl,
  'Ops Confidence': ops.confidence,
  'Normalize Company Name': stableStringify({ normalizedCompanyName: normalizedName }),
  'Normalized Name': normalizedName,
  Conference: original.conference,
  booth_number: original.booth_number,
  'Dedupe Key': [domain || normalizedName.toLowerCase(), String(original.conference || '').toLowerCase()].filter(Boolean).join('|'),
  'Source Row ID': original.sourceRecordId,
  'Enable Smartlead': original.enableSmartlead ? 'true' : 'false',
  'Smartlead Campaign ID': original.smartleadCampaignId,
  'Sales. ': isQualified(sales) ? 'queued' : 'skipped',
  'Ops.': isQualified(ops) ? 'queued' : 'skipped',
  'CS. ': isQualified(cs) ? 'queued' : 'skipped',
  'Updated At': new Date().toISOString(),
};
const leadDedupeKey = leadFields['Dedupe Key'];

return {
  json: {
    ...original,
    researchResponseRaw: response,
    researchJson: parsed,
    officialDomain: domain,
    normalizedName,
    sales,
    cs,
    ops,
    qualified: {
      sales: isQualified(sales),
      cs: isQualified(cs),
      ops: isQualified(ops),
    },
    leadDedupeKey,
    airtableLeadBody: {
      performUpsert: { fieldsToMergeOn: ['Dedupe Key'] },
      records: [{ fields: leadFields }],
      typecast: true,
    },
  }
};
"""


MATERIALIZE_CONTACTS_JS = r"""
function stableStringify(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch (error) {
    return String(value ?? '');
  }
}

function firstName(fullName, fallback) {
  return String(fallback || fullName || '').replace(/^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+/i, '').split(/\s+/).filter(Boolean)[0] || '';
}

function identityPart(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9@._+-]+/g, '-').replace(/^-+|-+$/g, '');
}

function contactItem(row, personaKey, clayPersona, tableId, person) {
  const fullName = person.fullName || '';
  const first = firstName(fullName, person.firstName);
  const base = {
    sourceLeadRecordId: row.sourceRecordId,
    sourceRowIndex: row.sourceRowIndex,
    leadAirtableRecordId: row.leadAirtableRecordId || '',
    persona: personaKey,
    clayPersona,
    contactAirtableTableId: tableId,
    brand_name: row.brand_name,
    companyName: row.brand_name,
    normalizedName: row.normalizedName,
    domain: row.officialDomain,
    exhibitor_url: row.exhibitor_url,
    company_email: row.company_email,
    showroom_contact: row.showroom_contact,
    conference: row.conference,
    booth_number: row.booth_number,
    fullName,
    firstName: first,
    lastName: fullName.split(/\s+/).slice(1).join(' '),
    jobTitle: person.jobTitle || '',
    sourceUrl: person.sourceUrl || '',
    confidence: person.confidence || '',
    linkedinUrl: person.linkedinUrl || '',
    linkedinActive: person.linkedinActive || 'NO',
    linkedinActivitySummary: person.linkedinActivitySummary || '',
    suppliedEmail: person.email || '',
    suppliedPhone: person.phone || '',
    contactSourceType: person.contactSourceType || 'researched_persona',
    smartleadCampaignId: row.smartleadCampaignId || '',
    cadenceEnrollmentDate: row.cadenceEnrollmentDate || new Date().toISOString().slice(0, 10),
    enableSmartlead: row.enableSmartlead === true,
  };
  base.contactDedupeKey = [
    identityPart(base.suppliedEmail || base.linkedinUrl || base.fullName),
    identityPart(base.domain || base.normalizedName),
  ].filter(Boolean).join('|');

  let fields;
  if (personaKey === 'Sales') {
    fields = {
      showroom_contact: base.showroom_contact,
      company_email: base.company_email,
      'Rows from: las_vegas_market_exhibitors': base.sourceLeadRecordId,
      brand_name: base.brand_name,
      exhibitor_url: base.exhibitor_url,
      'Official Company Domain (2)': base.domain,
      'Sales Job Title': base.jobTitle,
      'Sales Confidence': base.confidence,
      'Sales Full Name': base.fullName,
      'Sales Source URL': base.sourceUrl,
      'LinkedIn Profile URL': stableStringify({ linkedinUrl: base.linkedinUrl }),
      'Person LinkedIn': base.linkedinUrl,
      'Normalized Company Name': base.normalizedName,
      'First Name': stableStringify({ firstName: base.firstName }),
      'First Name (2)': base.firstName,
      'LinkedIn Activity Status': stableStringify({
        personLinkedin: base.linkedinUrl,
        isActive: base.linkedinActive,
        summary: base.linkedinActivitySummary,
      }),
      'Normalized Name': base.normalizedName,
      'Dedupe Key': base.contactDedupeKey,
      'Contact Email': base.suppliedEmail,
      'Contact Phone': base.suppliedPhone,
      'Contact Source Type': base.contactSourceType,
    };
  } else if (personaKey === 'Ops') {
    fields = {
      'Ops Full Name': base.fullName,
      brand_name: base.brand_name,
      'Ops Job Title': base.jobTitle,
      'Ops Source Url': base.sourceUrl,
      'Official Company Domain (2)': base.domain,
      'Ops Confidence': base.confidence,
      'Lookup Multiple Rows in Other Table (2)': '',
      'LinkedIn Profile URL': stableStringify({ linkedinUrl: base.linkedinUrl }),
      'Extract First Name': stableStringify({ firstName: base.firstName }),
      'First Name': base.firstName,
      'Normalized Company Name': base.normalizedName,
      'Normalized Name': base.normalizedName,
      company_email: base.company_email,
      exhibitor_url: base.exhibitor_url,
      showroom_contact: base.showroom_contact,
      'Rows from: InfoComm Market Leads': base.sourceLeadRecordId,
      Conference: base.conference,
      booth_number: base.booth_number,
      'Dedupe Key': base.contactDedupeKey,
      'Contact Email': base.suppliedEmail,
      'Contact Phone': base.suppliedPhone,
      'Contact Source Type': base.contactSourceType,
    };
  } else {
    fields = {
      brand_name: base.brand_name,
      'CS Confidence': base.confidence,
      'CS Job Title': base.jobTitle,
      exhibitor_url: base.exhibitor_url,
      'Official Company Domain (2)': base.domain,
      'CS Source URL': base.sourceUrl,
      'CS Full Name': base.fullName,
      'LinkedIn Profile URL': stableStringify({ linkedinUrl: base.linkedinUrl }),
      'Lookup Multiple Rows in Other Table': '',
      'Extract First Name': stableStringify({ First_Name: base.firstName, firstName: base.firstName }),
      First_Name: base.firstName,
      'Normalized Company Name': base.normalizedName,
      'Normalized Name': base.normalizedName,
      'Rows from: InfoComm Market Leads': base.sourceLeadRecordId,
      company_email: base.company_email,
      showroom_contact: base.showroom_contact,
      'Sales Confidence': row.sales?.confidence || '',
      'Sales Job Title': row.sales?.jobTitle || '',
      'Sales Full Name': row.sales?.fullName || '',
      'Sales Source URL': row.sales?.sourceUrl || '',
      booth_number: base.booth_number,
      Conference: base.conference,
      'Dedupe Key': base.contactDedupeKey,
      'Contact Email': base.suppliedEmail,
      'Contact Phone': base.suppliedPhone,
      'Contact Source Type': base.contactSourceType,
    };
  }

  return {
    json: {
      ...base,
      airtableContactBody: {
        performUpsert: { fieldsToMergeOn: ['Dedupe Key'] },
        records: [{ fields }],
        typecast: true,
      },
    }
  };
}

const parsedRows = $('Parse Research Result').all().map((entry) => entry.json);
const leadWriteRows = $input.all().map((entry) => entry.json);
const out = [];
for (let index = 0; index < parsedRows.length; index += 1) {
  const row = parsedRows[index];
  const leadWrite = leadWriteRows[index] || {};
  const leadRecord = Array.isArray(leadWrite.records) ? (leadWrite.records[0] || {}) : leadWrite;
  row.leadAirtableRecordId = leadRecord.id || leadRecord.recordId || '';
  const beforeCount = out.length;
  const scrapedPeople = row.scrapedContacts?.length
    ? row.scrapedContacts
    : (row.scrapedContact?.fullName ? [row.scrapedContact] : []);
  for (const scrapedPerson of scrapedPeople) {
    out.push(contactItem(row, 'Sales', 'trade_show_contact', 'tblh8mxFnH3xYuDkK', {
      ...scrapedPerson,
      confidence: 'Scraped',
      linkedinUrl: '',
      linkedinActive: 'NO',
      contactSourceType: 'scraped_trade_show_contact',
    }));
  }
  if (row.qualified.sales) out.push(contactItem(row, 'Sales', 'sales', 'tblh8mxFnH3xYuDkK', row.sales));
  if (row.qualified.ops) out.push(contactItem(row, 'Ops', 'ops', 'tblwk9D9Ve1RxwfNa', row.ops));
  if (row.qualified.cs) out.push(contactItem(row, 'CS', 'cs', 'tblRtquRALrH3ZXEk', row.cs));
  if (out.length === beforeCount) {
    out.push({
      json: {
        noQualifiedContacts: true,
        sourceLeadRecordId: row.sourceRecordId,
        sourceRowIndex: row.sourceRowIndex,
        leadAirtableRecordId: row.leadAirtableRecordId || '',
        brand_name: row.brand_name,
        companyName: row.brand_name,
        normalizedName: row.normalizedName,
        domain: row.officialDomain,
        conference: row.conference,
        booth_number: row.booth_number,
        skippedReason: 'No persona met the clean-contact threshold.',
        personaResults: {
          sales: row.sales,
          ops: row.ops,
          cs: row.cs,
        },
      }
    });
  }
}
return out;
"""


AFTER_CONTACT_WRITE_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function parseJson(value) {
  const raw = clean(value);
  if (!raw || !raw.startsWith('{')) return {};
  try {
    return JSON.parse(raw);
  } catch (error) {
    return {};
  }
}

function firstName(fullName, fallback) {
  return clean(fallback || fullName).replace(/^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+/i, '').split(/\s+/).filter(Boolean)[0] || '';
}

const rawResponse = item.json || {};
const response = Array.isArray(rawResponse.records) ? (rawResponse.records[0] || {}) : rawResponse;
const fields = response.fields || {};
const materialized = $('Materialize Contact Rows').item.json;
let persona = 'CS';
let contactAirtableTableId = 'tblRtquRALrH3ZXEk';
let fullName = clean(fields['CS Full Name']);
let jobTitle = clean(fields['CS Job Title']);
let sourceUrl = clean(fields['CS Source URL']);
let confidence = clean(fields['CS Confidence']);
let first = clean(fields.First_Name || parseJson(fields['Extract First Name']).firstName || parseJson(fields['Extract First Name']).First_Name);
let sourceLeadRecordId = clean(fields['Rows from: InfoComm Market Leads']);

if (fields['Sales Full Name'] !== undefined || fields['Sales Job Title'] !== undefined) {
  persona = 'Sales';
  contactAirtableTableId = 'tblh8mxFnH3xYuDkK';
  fullName = clean(fields['Sales Full Name']);
  jobTitle = clean(fields['Sales Job Title']);
  sourceUrl = clean(fields['Sales Source URL']);
  confidence = clean(fields['Sales Confidence']);
  first = clean(fields['First Name (2)'] || parseJson(fields['First Name']).firstName);
  sourceLeadRecordId = clean(fields['Rows from: las_vegas_market_exhibitors']);
} else if (fields['Ops Full Name'] !== undefined || fields['Ops Job Title'] !== undefined) {
  persona = 'Ops';
  contactAirtableTableId = 'tblwk9D9Ve1RxwfNa';
  fullName = clean(fields['Ops Full Name']);
  jobTitle = clean(fields['Ops Job Title']);
  sourceUrl = clean(fields['Ops Source Url']);
  confidence = clean(fields['Ops Confidence']);
  first = clean(fields.FirstName || fields['First Name'] || parseJson(fields['Extract First Name']).firstName);
  sourceLeadRecordId = clean(fields['Rows from: InfoComm Market Leads']);
}

const linkedinLookup = parseJson(fields['LinkedIn Profile URL']);
const linkedinUrl = clean(
  fields['Person LinkedIn']
  || linkedinLookup.linkedinUrl
  || linkedinLookup.personLinkedin
  || linkedinLookup.linkedin
);

return {
  json: {
    sourceLeadRecordId,
    leadAirtableRecordId: sourceLeadRecordId,
    persona,
    clayPersona: persona.toLowerCase(),
    contactAirtableTableId,
    brand_name: clean(fields.brand_name),
    companyName: clean(fields.brand_name),
    normalizedName: clean(fields['Normalized Name'] || fields['Normalized Company Name']),
    domain: clean(fields['Official Company Domain (2)']),
    exhibitor_url: clean(fields.exhibitor_url),
    company_email: clean(fields.company_email),
    showroom_contact: clean(fields.showroom_contact),
    conference: clean(fields.Conference),
    booth_number: clean(fields.booth_number),
    fullName,
    firstName: firstName(fullName, first),
    lastName: fullName.split(/\s+/).slice(1).join(' '),
    jobTitle,
    sourceUrl,
    confidence,
    linkedinUrl,
    linkedinActive: 'NO',
    linkedinActivitySummary: '',
    suppliedEmail: clean(fields['Contact Email']),
    suppliedPhone: clean(fields['Contact Phone']),
    contactSourceType: clean(fields['Contact Source Type']),
    enableSmartlead: materialized.enableSmartlead === true,
    smartleadCampaignId: materialized.smartleadCampaignId || '',
    cadenceEnrollmentDate: materialized.cadenceEnrollmentDate || new Date().toISOString().slice(0, 10),
    airtableContactRecordId: response.id || response.recordId || '',
    airtableContactCreateResponse: response,
  }
};
"""


BUILD_LINKEDIN_LOOKUP_REQUEST_JS = r"""
function compact(value) {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined && item !== null && item !== ''));
}

const row = item.json;
const model = (typeof $vars !== 'undefined' && $vars.CONDUIT_OPENAI_LINKEDIN_MODEL) || (typeof $vars !== 'undefined' && $vars.CONDUIT_OPENAI_MODEL) || 'gpt-4o-mini';
const userPrompt = `
Find the exact public LinkedIn profile URL for this trade-show prospect. Return only one valid JSON object.

Prospect:
${JSON.stringify(compact({
  full_name: row.fullName,
  job_title: row.jobTitle,
  company: row.companyName,
  normalized_company: row.normalizedName,
  domain: row.domain,
  existing_linkedin_url: row.linkedinUrl,
  source_url: row.sourceUrl
}), null, 2)}

Rules:
1. Return linkedinUrl only if it is an exact /in/ public profile for the same full name and current company/title context.
2. Do not return company pages, posts, sales-navigator URLs, directory pages, search URLs, or guessed LinkedIn slugs.
3. If the exact profile is not visible, return an empty linkedinUrl.
4. confidence must be High, Medium, Low, or Not Found.
5. Use High only when the profile visibly matches the person and company/title context.

JSON shape:
{"linkedinUrl":"","confidence":"","sourceUrl":"","reason":""}`;

return {
  json: {
    ...row,
    openaiLinkedinLookupRequest: {
      model,
      tools: [{ type: 'web_search' }],
      input: userPrompt
    }
  }
};
"""


PARSE_LINKEDIN_LOOKUP_JS = r"""
function outputText(response) {
  if (!response) return '';
  if (typeof response.output_text === 'string') return response.output_text;
  if (Array.isArray(response.output)) {
    return response.output.flatMap((entry) => {
      if (Array.isArray(entry.content)) {
        return entry.content.map((part) => part.text || part.output_text || '').filter(Boolean);
      }
      return [entry.text || ''];
    }).filter(Boolean).join('\n');
  }
  return response.text || response.message || '';
}

function parseJsonFromText(text) {
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (error) {
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return {};
    try {
      return JSON.parse(match[0]);
    } catch (nestedError) {
      return {};
    }
  }
}

function cleanLinkedInUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = raw.match(/^https?:\/\//i) ? raw : `https://${raw}`;
    const parsed = new URL(url);
    const host = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    if (!(host === 'linkedin.com' || host.endsWith('.linkedin.com')) || !parsed.pathname.toLowerCase().startsWith('/in/')) return '';
    const slug = parsed.pathname.split('/').filter(Boolean)[1] || '';
    if (!slug || /12345678|example|placeholder|your-name|first-last/i.test(slug)) return '';
    return `https://www.linkedin.com${parsed.pathname.replace(/\/+$/, '')}`;
  } catch (error) {
    const match = raw.match(/https?:\/\/(?:[\w-]+\.)?linkedin\.com\/in\/[^\s"')?#]+/i);
    if (!match) return '';
    const slug = match[0].split('/in/')[1]?.replace(/\/+$/, '') || '';
    if (!slug || /12345678|example|placeholder|your-name|first-last/i.test(slug)) return '';
    return `https://www.linkedin.com/in/${slug}`;
  }
}

function stableStringify(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch (error) {
    return String(value ?? '');
  }
}

function clean(value) {
  return String(value || '').trim();
}

function isHighConfidence(value) {
  return clean(value).toLowerCase().includes('high');
}

function sourceQualityScore(row, hasVerifiedLinkedin) {
  let score = 0;
  if (isHighConfidence(row.confidence)) score += 35;
  else if (clean(row.confidence).toLowerCase().includes('medium')) score += 20;
  if (clean(row.fullName).split(/\s+/).filter(Boolean).length >= 2) score += 10;
  if (clean(row.jobTitle) && !clean(row.jobTitle).toLowerCase().includes('not found')) score += 10;
  if (clean(row.sourceUrl)) score += 10;
  if (hasVerifiedLinkedin) score += 35;
  return Math.min(score, 100);
}

const source = $('Build LinkedIn Profile Lookup Request').item.json;
const response = item.json || {};
const text = outputText(response);
const parsed = parseJsonFromText(text);
const foundUrl = cleanLinkedInUrl(parsed.linkedinUrl || parsed.url || text);
const keptUrl = cleanLinkedInUrl(source.linkedinUrl);
const lookupConfidence = clean(parsed.confidence || '');
const linkedinUrl = foundUrl && isHighConfidence(lookupConfidence) ? foundUrl : '';
const rejectedLinkedInUrl = linkedinUrl ? '' : (foundUrl || keptUrl);
const verificationStatus = linkedinUrl
  ? 'verified_exact_high_confidence'
  : rejectedLinkedInUrl
  ? `rejected_${lookupConfidence ? lookupConfidence.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') : 'not_high_confidence'}`
  : 'missing_exact_profile';
const qualityScore = sourceQualityScore(source, Boolean(linkedinUrl));
const qualityNotes = [
  `LinkedIn verification: ${verificationStatus}`,
  lookupConfidence ? `Lookup confidence: ${lookupConfidence}` : '',
  rejectedLinkedInUrl ? `Rejected URL: ${rejectedLinkedInUrl}` : '',
  parsed.reason ? `Reason: ${parsed.reason}` : '',
].filter(Boolean).join('\n');
const lookup = {
  linkedinUrl,
  confidence: linkedinUrl ? lookupConfidence : (lookupConfidence || 'Not Found'),
  sourceUrl: parsed.sourceUrl || '',
  reason: parsed.reason || '',
  verificationStatus,
  rejectedLinkedInUrl,
  raw: parsed,
  error: response.error ? String(response.error.message || response.error.code || 'LinkedIn lookup error').slice(0, 1000) : '',
};
const fields = {
  'LinkedIn Profile URL': stableStringify(lookup),
  'LinkedIn Verification Status': verificationStatus,
  'LinkedIn Verification Confidence': lookup.confidence,
  'Quality Score': qualityScore,
  'Quality Notes': qualityNotes,
};
if (source.persona === 'Sales') {
  fields['Person LinkedIn'] = linkedinUrl;
}
return {
  json: {
    ...source,
    linkedinUrl,
    linkedinVerified: Boolean(linkedinUrl),
    linkedinVerificationStatus: verificationStatus,
    linkedinVerificationConfidence: lookup.confidence,
    rejectedLinkedInUrl,
    qualityScore,
    qualityNotes,
    linkedinActive: 'NO',
    linkedinActivitySummary: '',
    linkedinLookup: lookup,
    airtableLinkedInUpdateBody: {
      fields,
      typecast: true,
    },
  }
};
"""


AFTER_LINKEDIN_AIRTABLE_UPDATE_JS = r"""
const source = $('Parse LinkedIn Profile Lookup').item.json;
return {
  json: {
    ...source,
    airtableLinkedInUpdateResponse: item.json || {},
  }
};
"""


PREPARE_EMAIL_WATERFALL_JS = r"""
const suppliedEmail = String(item.json.suppliedEmail || '').trim().toLowerCase();
return {
  json: {
    ...item.json,
    leadMagicRaw: {},
    leadMagicEmail: '',
    leadMagicCatchAll: false,
    leadMagicValidationRaw: {},
    validLeadMagicEmail: '',
    wizaRaw: { skipped: true, reason: 'Wiza credential not available yet' },
    wizaEmail: '',
    wizaValidationRaw: {},
    validWizaEmail: '',
    icypeasStartRaw: {},
    icypeasReadRaw: {},
    icypeasEmail: '',
    prospeoRaw: {},
    prospeoEmail: '',
    findymailRaw: {},
    findymailEmail: '',
    finalProvider: suppliedEmail ? 'Scraped Contact' : '',
    finalWorkEmail: suppliedEmail,
    shouldRunLeadMagic: Boolean(!suppliedEmail && item.json.linkedinUrl && item.json.fullName && item.json.domain),
  }
};
"""


PARSE_LEADMAGIC_JS = r"""
function findFirstEmail(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return match ? match[0].toLowerCase() : '';
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const result = findFirstEmail(item);
      if (result) return result;
    }
  }
  if (typeof value === 'object') {
    for (const key of ['email', 'work_email', 'email_address', 'lead_email']) {
      const result = findFirstEmail(value[key]);
      if (result) return result;
    }
    for (const nested of Object.values(value)) {
      const result = findFirstEmail(nested);
      if (result) return result;
    }
  }
  return '';
}

function findCatchAll(value) {
  if (!value || typeof value !== 'object') return false;
  for (const [key, item] of Object.entries(value)) {
    const normalized = key.toLowerCase();
    if (normalized.includes('catch') && normalized.includes('all')) return Boolean(item);
    if (item && typeof item === 'object' && findCatchAll(item)) return true;
  }
  return false;
}

const source = $('Prepare Email Waterfall').item.json;
const raw = item.json || {};
const email = findFirstEmail(raw);
const catchAll = findCatchAll(raw);
return {
  json: {
    ...source,
    leadMagicRaw: raw,
    leadMagicEmail: email,
    leadMagicCatchAll: catchAll,
    validLeadMagicEmail: email && !catchAll ? email : '',
    shouldValidateLeadMagic: Boolean(email && catchAll),
  }
};
"""


PARSE_LEADMAGIC_VALIDATION_JS = r"""
function isValid(value) {
  if (!value) return false;
  const text = JSON.stringify(value).toLowerCase();
  if (text.includes('"valid":true') || text.includes('"is_valid":true')) return true;
  if (text.includes('"status":"valid"') || text.includes('"result":"valid"')) return true;
  if (text.includes('"deliverable"') && !text.includes('"undeliverable"')) return true;
  return false;
}

const source = $('Merge After LeadMagic').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    leadMagicValidationRaw: raw,
    validLeadMagicEmail: isValid(raw) ? source.leadMagicEmail : source.validLeadMagicEmail,
  }
};
"""


AFTER_LEADMAGIC_VALIDATION_JS = r"""
const row = item.json;
return {
  json: {
    ...row,
    shouldRunIcypeas: Boolean(row.linkedinUrl && !row.validLeadMagicEmail && row.fullName && row.domain),
  }
};
"""


PARSE_ICYPEAS_START_JS = r"""
const source = $('After LeadMagic Validation').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    icypeasStartRaw: raw,
    icypeasSearchId: raw.item?._id || raw._id || raw.id || '',
  }
};
"""


PARSE_ICYPEAS_READ_JS = r"""
function findEmail(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return match ? match[0].toLowerCase() : '';
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const result = findEmail(item);
      if (result) return result;
    }
  }
  if (typeof value === 'object') {
    for (const nested of Object.values(value)) {
      const result = findEmail(nested);
      if (result) return result;
    }
  }
  return '';
}

function firstEmail(raw) {
  const items = Array.isArray(raw.items) ? raw.items : [];
  for (const item of items) {
    const emails = item?.results?.emails || item?.emails || [];
    if (Array.isArray(emails) && emails.length) {
      const email = findEmail(emails);
      if (email) return email;
    }
  }
  return findEmail(raw);
}

const source = $('Wait for Icypeas').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    icypeasReadRaw: raw,
    icypeasEmail: firstEmail(raw),
  }
};
"""


AFTER_ICYPEAS_JS = r"""
const row = item.json;
return {
  json: {
    ...row,
    shouldRunProspeo: Boolean(row.linkedinUrl && !row.validLeadMagicEmail && !row.icypeasEmail && !row.validWizaEmail && row.fullName && row.domain),
    prospeoRequest: {
      only_verified_email: true,
      data: {
        full_name: row.fullName,
        company_name: row.companyName,
        company_website: row.domain,
        ...(row.linkedinUrl ? { linkedin_url: row.linkedinUrl } : {})
      }
    }
  }
};
"""


PARSE_PROSPEO_JS = r"""
function findEmail(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return match ? match[0].toLowerCase() : '';
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const result = findEmail(item);
      if (result) return result;
    }
  }
  if (typeof value === 'object') {
    for (const key of ['email', 'work_email', 'email_address']) {
      const result = findEmail(value[key]);
      if (result) return result;
    }
    for (const nested of Object.values(value)) {
      const result = findEmail(nested);
      if (result) return result;
    }
  }
  return '';
}

function emailFromProspeo(raw) {
  const person = raw.person || raw.data?.person || raw.result?.person || raw.data || raw;
  return findEmail(person);
}

const source = $('After Icypeas').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    prospeoRaw: raw,
    prospeoEmail: emailFromProspeo(raw),
  }
};
"""


AFTER_PROSPEO_JS = r"""
const row = item.json;
return {
  json: {
    ...row,
    shouldRunFindymail: Boolean(row.linkedinUrl && !row.validLeadMagicEmail && !row.icypeasEmail && !row.validWizaEmail && !row.prospeoEmail && row.fullName && row.domain),
  }
};
"""


PARSE_FINDYMAIL_JS = r"""
function findEmail(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return match ? match[0].toLowerCase() : '';
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const result = findEmail(item);
      if (result) return result;
    }
  }
  if (typeof value === 'object') {
    for (const key of ['email', 'email_address', 'work_email']) {
      const result = findEmail(value[key]);
      if (result) return result;
    }
    for (const nested of Object.values(value)) {
      const result = findEmail(nested);
      if (result) return result;
    }
  }
  return '';
}

const source = $('After Prospeo').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    findymailRaw: raw,
    findymailEmail: findEmail(raw),
  }
};
"""


MERGE_FINAL_EMAIL_JS = r"""
function stableStringify(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch (error) {
    return String(value ?? '');
  }
}

function cleanEmail(value) {
  if (typeof value !== 'string') return '';
  const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0].toLowerCase() : '';
}

function baseUpdates(row) {
  const common = {
    'Find work email': stableStringify(row.leadMagicRaw),
    'LeadMagic Email': row.leadMagicEmail,
    'Is Domain Catch All': row.leadMagicCatchAll ? 'true' : 'false',
    'Validate Email': stableStringify(row.leadMagicValidationRaw),
    'Find Work Email - Prospeo': stableStringify(row.prospeoRaw),
    'Prospeo Email': row.prospeoEmail,
    'Find Work Email - Findymail': stableStringify(row.findymailRaw),
    'Findymail Email': row.findymailEmail,
    'Final Work Email': row.finalWorkEmail,
  };

  if (row.persona === 'Sales') {
    return {
      ...common,
      'Find work email (2)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
      'Icypeas Email': row.icypeasEmail,
      'Valid LeadMagic Email': row.validLeadMagicEmail,
      'Find work email (3)': stableStringify(row.wizaRaw),
      'Wiza Email': row.wizaEmail,
      'Validate Email (2)': stableStringify(row.wizaValidationRaw),
      'Valid Wiza Email': row.validWizaEmail,
    };
  }

  if (row.persona === 'Ops') {
    return {
      ...common,
      'Valid Email': row.validLeadMagicEmail ? 'true' : 'false',
      'Valid Leadmagic Email': row.validLeadMagicEmail,
      'Find work email (2)': stableStringify(row.wizaRaw),
      'Wiza Email': row.wizaEmail,
      'Validate Email (2)': stableStringify(row.wizaValidationRaw),
      Valid: row.validWizaEmail ? 'true' : 'false',
      'Valid Wiza Email': row.validWizaEmail,
      'Find work email (3)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
      'Icypeas Email': row.icypeasEmail,
    };
  }

  return {
    ...common,
    'Valid LeadMagic Email': row.validLeadMagicEmail,
    'Find work email (2)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
    'Icypeas Email': row.icypeasEmail,
    'Find work email (3)': stableStringify(row.wizaRaw),
    'Wiza Email': row.wizaEmail,
    'Validate Email (2)': stableStringify(row.wizaValidationRaw),
    'Validate Wiza Email': row.validWizaEmail ? 'true' : 'false',
    'Valid Wiza Email': row.validWizaEmail,
  };
}

const row = item.json;
const salesOrder = [
  ['Scraped Contact', cleanEmail(row.suppliedEmail)],
  ['LeadMagic', cleanEmail(row.validLeadMagicEmail)],
  ['Icypeas', cleanEmail(row.icypeasEmail)],
  ['Wiza', cleanEmail(row.validWizaEmail)],
  ['Prospeo', cleanEmail(row.prospeoEmail)],
  ['Findymail', cleanEmail(row.findymailEmail)],
];
const opsCsOrder = [
  ['Scraped Contact', cleanEmail(row.suppliedEmail)],
  ['LeadMagic', cleanEmail(row.validLeadMagicEmail)],
  ['Wiza', cleanEmail(row.validWizaEmail)],
  ['Icypeas', cleanEmail(row.icypeasEmail)],
  ['Prospeo', cleanEmail(row.prospeoEmail)],
  ['Findymail', cleanEmail(row.findymailEmail)],
];
const winner = (row.persona === 'Sales' ? salesOrder : opsCsOrder).find(([, email]) => Boolean(email)) || ['None', ''];
const finalRow = {
  ...row,
  finalProvider: winner[0],
  finalWorkEmail: winner[1],
  finalEmailValidationRaw: {},
  finalEmailValidationStatus: winner[1] ? 'pending' : 'skipped_no_email',
  shouldValidateFinalEmail: Boolean(winner[1]),
};

return {
  json: {
    ...finalRow,
    shouldPushSmartlead: Boolean(
      finalRow.finalWorkEmail
      && finalRow.enableSmartlead === true
      && String(finalRow.smartleadCampaignId || '').trim()
    ),
    shouldRunApify: Boolean(finalRow.linkedinUrl),
    airtableContactUpdateBody: {
      fields: baseUpdates(finalRow),
      typecast: true,
    }
  }
};
"""


PARSE_FINAL_EMAIL_VALIDATION_JS = r"""
function isValid(value) {
  if (!value) return false;
  const text = JSON.stringify(value).toLowerCase();
  if (text.includes('"valid":true') || text.includes('"is_valid":true')) return true;
  if (text.includes('"status":"valid"') || text.includes('"result":"valid"')) return true;
  if (text.includes('"state":"valid"') || text.includes('"verdict":"valid"')) return true;
  if (text.includes('"deliverable"') && !text.includes('"undeliverable"')) return true;
  return false;
}

function isExplicitInvalid(value) {
  if (!value) return false;
  const text = JSON.stringify(value).toLowerCase();
  if (text.includes('"valid":false') || text.includes('"is_valid":false')) return true;
  if (text.includes('"status":"invalid"') || text.includes('"result":"invalid"')) return true;
  if (text.includes('"state":"invalid"') || text.includes('"verdict":"invalid"')) return true;
  if (text.includes('"undeliverable"')) return true;
  return false;
}

const source = $('Merge Final Email').item.json;
const raw = item.json || {};
return {
  json: {
    ...source,
    finalEmailValidationRaw: raw,
    finalEmailValidationStatus: isValid(raw) ? 'valid' : isExplicitInvalid(raw) ? 'invalid' : 'unknown',
  }
};
"""


APPLY_FINAL_EMAIL_VALIDATION_JS = r"""
function stableStringify(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch (error) {
    return String(value ?? '');
  }
}

function baseUpdates(row) {
  const validateEmailPayload = {
    leadMagic: row.leadMagicValidationRaw || {},
    final: row.finalEmailValidationRaw || {},
    finalStatus: row.finalEmailValidationStatus || '',
    rejectedFinalProvider: row.rejectedFinalProvider || '',
    rejectedFinalEmail: row.rejectedFinalEmail || '',
  };
  const common = {
    'Find work email': stableStringify(row.leadMagicRaw),
    'LeadMagic Email': row.leadMagicEmail,
    'Is Domain Catch All': row.leadMagicCatchAll ? 'true' : 'false',
    'Validate Email': stableStringify(validateEmailPayload),
    'Find Work Email - Prospeo': stableStringify(row.prospeoRaw),
    'Prospeo Email': row.prospeoEmail,
    'Find Work Email - Findymail': stableStringify(row.findymailRaw),
    'Findymail Email': row.findymailEmail,
    'Final Work Email': row.finalWorkEmail,
  };

  if (row.persona === 'Sales') {
    return {
      ...common,
      'Find work email (2)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
      'Icypeas Email': row.icypeasEmail,
      'Valid LeadMagic Email': row.validLeadMagicEmail,
      'Find work email (3)': stableStringify(row.wizaRaw),
      'Wiza Email': row.wizaEmail,
      'Validate Email (2)': stableStringify(row.wizaValidationRaw),
      'Valid Wiza Email': row.validWizaEmail,
    };
  }

  if (row.persona === 'Ops') {
    return {
      ...common,
      'Valid Email': row.validLeadMagicEmail ? 'true' : 'false',
      'Valid Leadmagic Email': row.validLeadMagicEmail,
      'Find work email (2)': stableStringify(row.wizaRaw),
      'Wiza Email': row.wizaEmail,
      'Validate Email (2)': stableStringify(row.wizaValidationRaw),
      Valid: row.validWizaEmail ? 'true' : 'false',
      'Valid Wiza Email': row.validWizaEmail,
      'Find work email (3)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
      'Icypeas Email': row.icypeasEmail,
    };
  }

  return {
    ...common,
    'Valid LeadMagic Email': row.validLeadMagicEmail,
    'Find work email (2)': stableStringify(row.icypeasReadRaw || row.icypeasStartRaw),
    'Icypeas Email': row.icypeasEmail,
    'Find work email (3)': stableStringify(row.wizaRaw),
    'Wiza Email': row.wizaEmail,
    'Validate Email (2)': stableStringify(row.wizaValidationRaw),
    'Validate Wiza Email': row.validWizaEmail ? 'true' : 'false',
    'Valid Wiza Email': row.validWizaEmail,
  };
}

const source = item.json;
const rejected = source.finalWorkEmail && source.finalEmailValidationStatus === 'invalid';
const row = rejected
  ? {
      ...source,
      rejectedFinalProvider: source.finalProvider,
      rejectedFinalEmail: source.finalWorkEmail,
      finalProvider: 'None',
      finalWorkEmail: '',
    }
  : source;

return {
  json: {
    ...row,
    shouldPushSmartlead: Boolean(
      row.finalWorkEmail
      && row.enableSmartlead === true
      && row.finalEmailValidationStatus === 'valid'
      && String(row.smartleadCampaignId || '').trim()
    ),
    smartleadSkippedReason: !row.finalWorkEmail
      ? 'No validated final work email.'
      : row.enableSmartlead !== true
      ? 'Smartlead disabled for this run.'
      : row.finalEmailValidationStatus !== 'valid'
      ? 'Final work email is not validated.'
      : !String(row.smartleadCampaignId || '').trim()
      ? 'Missing required show-specific Smartlead campaign ID.'
      : '',
    shouldRunApify: Boolean(row.linkedinUrl),
    airtableContactUpdateBody: {
      fields: baseUpdates(row),
      typecast: true,
    }
  }
};
"""


AFTER_EMAIL_AIRTABLE_UPDATE_JS = r"""
const source = $('Apply Final Email Validation').item.json;
return {
  json: {
    ...source,
    airtableEmailUpdateResponse: item.json || {},
  }
};
"""


PARSE_SMARTLEAD_JS = r"""
const source = $('Need Smartlead?').item.json;
return {
  json: {
    ...source,
    smartleadRaw: item.json || {},
  }
};
"""


AFTER_SMARTLEAD_JS = r"""
const row = item.json;
let smartleadField = '';
try {
  smartleadField = JSON.stringify({
    campaignId: row.smartleadCampaignId || '',
    cadenceEnrollmentDate: row.cadenceEnrollmentDate || '',
    attempted: Boolean(row.smartleadRaw),
    response: row.smartleadRaw || { skipped: true, reason: row.smartleadSkippedReason || 'Smartlead skipped.' },
  }, null, 2);
} catch (error) {
  smartleadField = String(row.smartleadRaw || row.smartleadSkippedReason || 'Smartlead skipped.');
}
const fieldName = row.persona === 'Sales' ? 'Atlanta Market Campaign' : 'Add Lead to Campaign';
return {
  json: {
    ...row,
    airtableSmartleadUpdateBody: {
      fields: {
        [fieldName]: smartleadField,
      },
      typecast: true,
    },
  }
};
"""


AFTER_SMARTLEAD_AIRTABLE_UPDATE_JS = r"""
const source = $('After Smartlead').item.json;
return {
  json: {
    ...source,
    airtableSmartleadUpdateResponse: item.json || {},
  }
};
"""


PARSE_APIFY_JS = r"""
const sourceItems = $('Need Apify?').all();
const source = sourceItems[$itemIndex]?.json || {};
const raw = item.json || {};
let parsed = raw;
if (typeof raw.apifyResponseText === 'string') {
  try {
    parsed = JSON.parse(raw.apifyResponseText || '[]');
  } catch (error) {
    parsed = { rawText: raw.apifyResponseText };
  }
}
return {
  json: {
    ...source,
    apifyRaw: parsed,
  }
};
"""


AFTER_APIFY_JS = r"""
const row = item.json;
const apifyWasRun = row.shouldRunApify === true && Object.prototype.hasOwnProperty.call(row, 'apifyRaw');
const apifyResult = apifyWasRun
  ? (row.apifyRaw || {})
  : { skipped: true, reason: 'No verified LinkedIn profile URL; Apify skipped.' };
let apifyField = '';
try {
  apifyField = JSON.stringify(apifyResult, null, 2);
} catch (error) {
  apifyField = String(apifyResult || '');
}
const apifyItems = Array.isArray(apifyResult) ? apifyResult : [];
function itemTimestamp(value) {
  if (!value || typeof value !== 'object') return 0;
  if (Number.isFinite(Number(value.createdAtTimestamp))) return Number(value.createdAtTimestamp);
  const parsed = Date.parse(value.createdAt || value.postedAt || '');
  return Number.isFinite(parsed) ? parsed : 0;
}
const latestActivityTs = apifyItems.reduce((latest, entry) => Math.max(latest, itemTimestamp(entry)), 0);
const activeWindowMs = 365 * 24 * 60 * 60 * 1000;
const apifyVerifiedActivity = Boolean(latestActivityTs && (Date.now() - latestActivityTs) <= activeWindowMs);
const latestActivityAt = latestActivityTs ? new Date(latestActivityTs).toISOString() : '';
const activitySummary = !apifyWasRun
  ? 'No verified LinkedIn profile URL; Apify skipped.'
  : apifyItems.length === 0
  ? 'Apify returned no LinkedIn reaction/activity items for the supplied profile.'
  : apifyVerifiedActivity
  ? `Apify returned ${apifyItems.length} LinkedIn reaction/activity item(s); latest visible activity was ${latestActivityAt}.`
  : `Apify returned ${apifyItems.length} LinkedIn reaction/activity item(s), but latest visible activity was stale (${latestActivityAt}).`;
const apifyVerificationStatus = !row.linkedinUrl
  ? 'missing_exact_profile'
  : apifyVerifiedActivity
  ? 'verified_active'
  : 'verified_no_recent_activity';
const finalQualityScore = Math.min(100, Number(row.qualityScore || 0) + (apifyVerifiedActivity ? 20 : 0));
const finalQualityNotes = [
  row.qualityNotes || '',
  `Apify verification: ${apifyVerificationStatus}`,
  activitySummary,
].filter(Boolean).join('\n');
const fields = {
  'Run Apify Actor': apifyField,
  'LinkedIn Verification Status': apifyVerificationStatus,
  'Quality Score': finalQualityScore,
  'Quality Notes': finalQualityNotes,
};
if (row.persona === 'Sales') {
  fields['LinkedIn Activity Status'] = JSON.stringify({
    personLinkedin: row.linkedinUrl || '',
    isActive: apifyVerifiedActivity ? 'YES' : 'NO',
    source: 'Apify',
    latestActivityAt,
    summary: activitySummary,
  }, null, 2);
}
return {
  json: {
    ...row,
    linkedinActive: apifyVerifiedActivity ? 'YES' : 'NO',
    linkedinVerificationStatus: apifyVerificationStatus,
    linkedinActivitySummary: activitySummary,
    latestLinkedInActivityAt: latestActivityAt,
    qualityScore: finalQualityScore,
    qualityNotes: finalQualityNotes,
    airtableApifyUpdateBody: {
      fields,
      typecast: true,
    },
  }
};
"""


AFTER_APIFY_AIRTABLE_UPDATE_JS = r"""
const source = $('After Apify').item.json;
return {
  json: {
    ...source,
    airtableApifyUpdateResponse: item.json || {},
  }
};
"""


BUILD_PIPEDRIVE_SYNC_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function cleanEmail(value) {
  const email = clean(value).toLowerCase();
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email) ? email : '';
}

function cleanPhone(value) {
  const phone = clean(value);
  if (!phone || isBadPlaceholder(phone)) return '';
  const digits = phone.replace(/\D/g, '');
  return digits.length >= 7 && digits.length <= 15 ? phone : '';
}

function slug(value) {
  return clean(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'trade_show';
}

function identityPart(value) {
  return clean(value)
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/^www\./g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function keyFor(row) {
  return clean(row.airtableContactRecordId)
    || [row.persona, row.companyName, row.fullName, row.sourceLeadRecordId].map(clean).join('|');
}

function finiteIndex(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.floor(number) : null;
}

function escapeHtml(value) {
  return clean(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function noteLine(label, value) {
  const text = clean(value);
  return text ? `<strong>${escapeHtml(label)}:</strong> ${escapeHtml(text)}` : '';
}

function isBadPlaceholder(value) {
  const text = clean(value).toLowerCase();
  if (!text) return false;
  return [
    'not found',
    'missing input',
    'no qualifying',
    'no phone found',
    'none',
    'n/a',
    'unknown',
    'placeholder',
    'example',
    'the provided',
    'identified top',
    'per instructions',
  ].some((needle) => text.includes(needle)) || /\{\{.+\}\}|\$json|^\[[^\]]+\]$/.test(text);
}

function validLinkedInProfile(value) {
  let raw = clean(value);
  if (!raw) return '';
  if (raw.startsWith('{')) {
    try {
      const parsed = JSON.parse(raw);
      raw = clean(parsed.linkedinUrl || parsed.personLinkedin || parsed.linkedin || parsed.raw?.linkedinUrl || raw);
    } catch (error) {
      // Fall through to regex extraction.
    }
  }
  const match = raw.match(/(?:https?:\/\/)?(?:[\w-]+\.)?linkedin\.com\/in\/([A-Za-z0-9_%.-]+)/i);
  if (!match) return '';
  const slugPart = clean(match[1]).replace(/\/+$/, '');
  if (!slugPart || /12345678|example|placeholder|your-name|first-last/i.test(slugPart)) return '';
  const normalized = `https://www.linkedin.com/in/${slugPart}`;
  return isBadPlaceholder(normalized) ? '' : normalized;
}

let completedRows = [];
try {
  completedRows = $('After Apify Airtable Update').all()
    .map((entry) => entry.json)
    .filter((row) => Boolean(
      !row.noQualifiedContacts
      && clean(row.fullName)
      && clean(row.companyName)
      && (
        (clean(row.finalEmailValidationStatus).toLowerCase() === 'valid' && cleanEmail(row.finalWorkEmail))
        || cleanPhone(row.suppliedPhone || row.contactPhone || row.phone)
        || validLinkedInProfile(row.linkedinUrl)
      )
    ));
} catch (error) {
  completedRows = [];
}

const row = item.json || {};
const fullName = clean(row.fullName);
const companyName = clean(row.normalizedName || row.companyName);
const finalWorkEmail = clean(row.finalEmailValidationStatus).toLowerCase() === 'valid'
  ? cleanEmail(row.finalWorkEmail)
  : '';
const finalPhone = cleanPhone(row.suppliedPhone || row.contactPhone || row.phone);
const linkedinUrl = validLinkedInProfile(row.linkedinUrl);
const showName = clean(row.conference || row.showName || row.rawLead?.Conference || row.rawLead?.conference || 'trade_show');
const showContext = showName === 'trade_show' ? 'Trade Show' : showName;
const sourceChannelId = `preshow_${slug(showName)}`;
const pipedriveRowIdentity = [
  'preshow',
  identityPart(showContext),
  identityPart(row.persona),
  identityPart(companyName),
  identityPart(fullName),
  identityPart(linkedinUrl || finalWorkEmail || finalPhone),
].filter(Boolean).join(':');
const currentKey = keyFor(row);
const completedIndex = completedRows.findIndex((candidate) => keyFor(candidate) === currentKey);
const n8nItemIndex = typeof $itemIndex === 'undefined' ? null : finiteIndex($itemIndex);
const sourceRowIndex = finiteIndex(row.sourceRowIndex);
const assignmentIndex = completedIndex >= 0 ? completedIndex : (n8nItemIndex ?? sourceRowIndex ?? 0);
const reps = [
  { name: 'Lea Skoumbakis', id: 25200571 },
  { name: 'Austin Weitman', id: 25188570 },
];
const rep = reps[assignmentIndex % reps.length];
const invalidPerson = isBadPlaceholder(fullName) || isBadPlaceholder(row.jobTitle);
const hasReachableChannel = Boolean(finalWorkEmail || finalPhone || linkedinUrl);
const shouldSyncPipedrive = Boolean(fullName && companyName && hasReachableChannel && !invalidPerson);

let skipReason = '';
if (!shouldSyncPipedrive) {
  if (!fullName) skipReason = 'Missing completed contact name.';
  else if (!companyName) skipReason = 'Missing completed company name.';
  else if (isBadPlaceholder(fullName) || isBadPlaceholder(row.jobTitle)) skipReason = 'Contact contains unresolved placeholder text.';
  else if (!hasReachableChannel) skipReason = 'Missing a valid final email, phone, or LinkedIn profile.';
  else skipReason = 'Contact is not complete enough for Pipedrive.';
}

const leadTitle = `${companyName} - ${fullName} - ${showContext}`;
const legacyLeadTitle = `${companyName} - ${fullName}`;
const activityBaseNote = [
  noteLine('Show', showContext),
  noteLine('Cadence enrollment date', row.cadenceEnrollmentDate),
  noteLine('Source channel id', sourceChannelId),
  noteLine('Assigned owner', `${rep.name} (${rep.id})`),
  noteLine('Persona', row.persona),
  noteLine('Company', companyName),
  noteLine('Domain', row.domain),
  noteLine('Website', row.website || (row.domain ? `https://${row.domain}` : '')),
  noteLine('Person', fullName),
  noteLine('Title', row.jobTitle),
  noteLine('Email', finalWorkEmail),
  noteLine('Phone', finalPhone),
  noteLine('Email provider', row.finalProvider),
  noteLine('Email validation', row.finalEmailValidationStatus),
  noteLine('LinkedIn', linkedinUrl),
  noteLine('LinkedIn active', row.linkedinActive),
  noteLine('Latest LinkedIn activity', row.latestLinkedInActivityAt),
  noteLine('LinkedIn activity summary', row.linkedinActivitySummary),
  noteLine('Company source URL', row.sourceUrl),
  noteLine('Booth', row.booth_number),
  noteLine('Airtable lead record', row.leadAirtableRecordId || row.sourceLeadRecordId),
  noteLine('Airtable contact record', row.airtableContactRecordId),
].filter(Boolean).join('<br>');

return {
  json: {
    ...row,
    linkedinUrl,
    finalWorkEmail,
    finalPhone,
    shouldSyncPipedrive,
    pipedriveSyncStatus: shouldSyncPipedrive ? 'pending' : 'skipped',
    pipedriveSkipReason: skipReason,
    pipedriveOwnerName: rep.name,
    pipedriveOwnerId: rep.id,
    pipedriveAssignmentIndex: assignmentIndex,
    pipedriveRowIdentity,
    pipedriveSourceChannelId: sourceChannelId,
    pipedriveShowContext: showContext,
    pipedriveLeadTitle: leadTitle,
    pipedriveLegacyLeadTitle: legacyLeadTitle,
    pipedriveActivityBaseNote: activityBaseNote,
    pipedriveOrgSearchTerm: companyName,
    pipedrivePersonSearchTerm: finalWorkEmail || finalPhone || fullName,
    pipedrivePersonSearchFields: finalWorkEmail ? 'email' : finalPhone ? 'phone' : 'name',
    pipedriveOriginId: pipedriveRowIdentity || clean(row.airtableContactRecordId || row.waterfallKey || row.sourceRecordId),
  }
};
"""


PARSE_PIPEDRIVE_ORG_SEARCH_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function normalizeName(value) {
  return clean(value)
    .toLowerCase()
    .replace(/\b(incorporated|inc\.?|llc|l\.l\.c\.|ltd\.?|co\.?|corp\.?|corporation|company)\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

const source = $('Build Pipedrive Sync').item.json;
const raw = item.json || {};
const items = Array.isArray(raw.data?.items)
  ? raw.data.items
  : Array.isArray(raw.data)
  ? raw.data
  : [];
const target = normalizeName(source.pipedriveOrgSearchTerm || source.companyName);
const candidates = items.map((entry) => entry.item || entry).filter(Boolean);
const match = candidates.find((candidate) => normalizeName(candidate.name) === target) || candidates[0] || null;
const id = match?.id || '';
return {
  json: {
    ...source,
    pipedriveOrgId: id ? Number(id) : '',
    pipedriveOrgStatus: id ? 'reused' : 'not_found',
    pipedriveOrgSearchError: raw.error?.message || raw.message || '',
  }
};
"""


PARSE_PIPEDRIVE_ORG_CREATE_JS = r"""
const source = $('Need Create Pipedrive Org?').item.json;
const raw = item.json || {};
const id = raw.data?.id || raw.id || source.pipedriveOrgId || '';
const status = source.pipedriveOrgId ? 'updated' : 'created';
return {
  json: {
    ...source,
    pipedriveOrgId: id ? Number(id) : '',
    pipedriveOrgStatus: id ? status : 'error',
    pipedriveOrgError: id ? '' : (raw.error?.message || raw.message || 'Pipedrive organization upsert did not return an id.'),
  }
};
"""


PARSE_PIPEDRIVE_PERSON_SEARCH_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function cleanEmail(value) {
  const email = clean(value).toLowerCase();
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email) ? email : '';
}

function normalizeName(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function candidateEmails(candidate) {
  const values = [];
  if (candidate.email) values.push(candidate.email);
  if (candidate.primary_email) values.push(candidate.primary_email);
  for (const entry of candidate.emails || []) {
    if (typeof entry === 'string') values.push(entry);
    else if (entry?.value) values.push(entry.value);
  }
  return values.map(cleanEmail).filter(Boolean);
}

const source = $('Merge Pipedrive Org').item.json;
const raw = item.json || {};
const items = Array.isArray(raw.data?.items)
  ? raw.data.items
  : Array.isArray(raw.data)
  ? raw.data
  : [];
const candidates = items.map((entry) => entry.item || entry).filter(Boolean);
const email = cleanEmail(source.finalWorkEmail);
const targetName = normalizeName(source.fullName);
let match = null;
if (email) {
  match = candidates.find((candidate) => candidateEmails(candidate).includes(email)) || null;
}
if (!match) {
  match = candidates.find((candidate) => normalizeName(candidate.name) === targetName) || null;
}
const id = match?.id || '';
return {
  json: {
    ...source,
    pipedrivePersonId: id ? Number(id) : '',
    pipedrivePersonStatus: id ? 'reused' : 'not_found',
    pipedrivePersonSearchError: raw.error?.message || raw.message || '',
  }
};
"""


PARSE_PIPEDRIVE_PERSON_CREATE_JS = r"""
const source = $('Need Create Pipedrive Person?').item.json;
const raw = item.json || {};
const id = raw.data?.id || raw.id || source.pipedrivePersonId || '';
const status = source.pipedrivePersonId ? 'updated' : 'created';
return {
  json: {
    ...source,
    pipedrivePersonId: id ? Number(id) : '',
    pipedrivePersonStatus: id ? status : 'error',
    pipedrivePersonError: id ? '' : (raw.error?.message || raw.message || 'Pipedrive person upsert did not return an id.'),
  }
};
"""


PARSE_PIPEDRIVE_TRADESHOW_FIELD_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function optionKey(value) {
  return clean(value)
    .toLowerCase()
    .replace(/\b20\d{2}\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

const source = $('Merge Pipedrive Person').item.json;
const raw = item.json || {};
const options = Array.isArray(raw.data?.options) ? raw.data.options : [];
const target = optionKey(source.pipedriveShowContext);
const match = options.find((option) => optionKey(option.label || option.name) === target) || null;
const id = match?.id || '';
return {
  json: {
    ...source,
    pipedriveTradeshowOptionId: id ? Number(id) : '',
    shouldCreatePipedriveTradeshowOption: Boolean(source.shouldSyncPipedrive && !id && target),
    pipedriveTradeshowOptionStatus: id ? 'reused' : 'not_found',
    pipedriveTradeshowOptionError: raw.error?.message || raw.message || '',
  }
};
"""


PARSE_PIPEDRIVE_TRADESHOW_OPTION_CREATE_JS = r"""
const source = $('Need Create Pipedrive Tradeshow Option?').item.json;
const raw = item.json || {};
const rows = Array.isArray(raw.data) ? raw.data : [];
const first = rows[0] || raw.data || {};
const id = first?.id || raw.id || source.pipedriveTradeshowOptionId || '';
return {
  json: {
    ...source,
    pipedriveTradeshowOptionId: id ? Number(id) : '',
    pipedriveTradeshowOptionStatus: id ? 'created' : 'error',
    pipedriveTradeshowOptionError: id ? '' : (raw.error?.message || raw.message || 'Pipedrive trade-show option create did not return an id.'),
  }
};
"""


PARSE_PIPEDRIVE_LEAD_SEARCH_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

const source = $('Merge Pipedrive Tradeshow Option').item.json;
const raw = item.json || {};
const rows = Array.isArray(raw.data) ? raw.data : [];
const wanted = new Set([source.pipedriveLeadTitle, source.pipedriveLegacyLeadTitle].map((title) => clean(title).toLowerCase()).filter(Boolean));
const match = rows.find((lead) => {
  const title = clean(lead.title).toLowerCase();
  const status = clean(lead.status || 'open').toLowerCase();
  return wanted.has(title) && status !== 'deleted';
}) || null;
const id = match?.id || '';
return {
  json: {
    ...source,
    pipedriveLeadId: id || '',
    pipedriveLeadStatus: id ? 'reused' : 'not_found',
    pipedriveLeadSearchError: raw.error?.message || raw.message || '',
  }
};
"""


PARSE_PIPEDRIVE_LEAD_UPSERT_JS = r"""
const source = $('Need Create Pipedrive Lead?').item.json;
const raw = item.json || {};
const id = raw.data?.id || raw.id || source.pipedriveLeadId || '';
const status = source.pipedriveLeadId ? 'updated' : 'created';
return {
  json: {
    ...source,
    pipedriveLeadId: id || '',
    pipedriveLeadStatus: id ? status : 'error',
    pipedriveLeadError: id ? '' : (raw.error?.message || raw.message || 'Pipedrive lead upsert did not return an id.'),
  }
};
"""


MATERIALIZE_PIPEDRIVE_ACTIVITIES_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

function enrollmentDate(value) {
  const raw = clean(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const parsed = new Date(`${raw}T00:00:00Z`);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  const today = new Date();
  return new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
}

function dueDate(start, offsetDays) {
  const date = new Date(start.getTime());
  date.setUTCDate(date.getUTCDate() + offsetDays);
  return date.toISOString().slice(0, 10);
}

const schedule = [
  { key: 'linkedin_day_4', offsetDays: 4, type: 'task', action: 'Send LinkedIn message', label: 'LinkedIn message' },
  { key: 'call_1_day_5', offsetDays: 5, type: 'call', action: 'Call lead (first call)', label: 'Call 1' },
  { key: 'call_2_day_7', offsetDays: 7, type: 'call', action: 'Call lead (second call)', label: 'Call 2' },
];

const out = [];
for (const [inputIndex, entry] of $input.all().entries()) {
  const row = entry.json || {};
  if (!row.shouldSyncPipedrive || !row.pipedriveLeadId) continue;
  const start = enrollmentDate(row.cadenceEnrollmentDate);
  const startIso = start.toISOString().slice(0, 10);
  for (const activity of schedule) {
    const subject = `${activity.label} - ${row.fullName || row.companyName} - ${row.pipedriveShowContext} [Pre-show D${activity.offsetDays}]`;
    const note = [
      `<strong>Action:</strong> ${activity.action}`,
      `<strong>Cadence step:</strong> Day ${activity.offsetDays}`,
      `<strong>Cadence enrollment date:</strong> ${startIso}`,
      row.pipedriveActivityBaseNote || '',
    ].filter(Boolean).join('<br>');
    out.push({
      json: {
        ...row,
        cadenceEnrollmentDate: startIso,
        pipedriveActivityKey: activity.key,
        pipedriveActivityOffsetDays: activity.offsetDays,
        pipedriveActivityType: activity.type,
        pipedriveActivitySubject: subject,
        pipedriveActivityDueDate: dueDate(start, activity.offsetDays),
        pipedriveActivityNote: note,
      },
      pairedItem: { item: inputIndex },
    });
  }
}
return out;
"""


PARSE_PIPEDRIVE_ACTIVITY_SEARCH_JS = r"""
function clean(value) {
  return String(value || '').trim();
}

const source = $('Materialize Pipedrive Activities').item.json;
const raw = item.json || {};
const rows = Array.isArray(raw.data?.items)
  ? raw.data.items
  : Array.isArray(raw.data)
  ? raw.data
  : [];
const target = clean(source.pipedriveActivitySubject).toLowerCase();
const legacy = source.pipedriveActivityKey === 'linkedin_day_4'
  ? `dm ${clean(source.fullName || source.companyName).toLowerCase()} on linkedin`
  : '';
const match = rows.find((activity) => {
  const subject = clean(activity.subject).toLowerCase();
  return subject === target || (legacy && (subject === legacy || subject.startsWith(`${legacy} -`)));
}) || null;
const id = match?.id || '';
return {
  json: {
    ...source,
    pipedriveActivityId: id ? Number(id) : '',
    pipedriveActivityStatus: id ? 'reused' : 'not_found',
    pipedriveActivitySearchError: raw.error?.message || raw.message || '',
  }
};
"""


PARSE_PIPEDRIVE_ACTIVITY_UPSERT_JS = r"""
const source = $('Need Create Pipedrive Activity?').item.json;
const raw = item.json || {};
const id = raw.data?.id || raw.id || source.pipedriveActivityId || '';
const responseError = raw.error?.message || raw.message || '';
const status = responseError
  ? 'error'
  : source.pipedriveActivityId
  ? 'updated'
  : id
  ? 'created'
  : 'error';
return {
  json: {
    ...source,
    pipedriveActivityId: id ? Number(id) : '',
    pipedriveActivityStatus: status,
    pipedriveActivityError: status === 'error'
      ? (responseError || 'Pipedrive activity upsert did not return an id.')
      : '',
  }
};
"""


SUMMARIZE_PIPEDRIVE_ACTIVITIES_JS = r"""
const groups = new Map();
for (const entry of $input.all()) {
  const row = entry.json || {};
  const key = row.pipedriveRowIdentity || row.airtableContactRecordId || row.sourceRecordId;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(row);
}

const out = [];
for (const rows of groups.values()) {
  const ordered = rows.slice().sort((a, b) => Number(a.pipedriveActivityOffsetDays || 0) - Number(b.pipedriveActivityOffsetDays || 0));
  const base = ordered[0] || {};
  const activities = ordered.map((row) => ({
    key: row.pipedriveActivityKey || '',
    offsetDays: Number(row.pipedriveActivityOffsetDays || 0),
    type: row.pipedriveActivityType || '',
    subject: row.pipedriveActivitySubject || '',
    dueDate: row.pipedriveActivityDueDate || '',
    id: row.pipedriveActivityId || '',
    status: row.pipedriveActivityStatus || '',
    error: row.pipedriveActivityError || row.pipedriveActivitySearchError || '',
  }));
  const ids = activities.map((activity) => activity.id).filter(Boolean);
  const errors = activities.map((activity) => activity.error).filter(Boolean);
  out.push({
    json: {
      ...base,
      pipedriveActivities: activities,
      pipedriveActivityIds: ids,
      pipedriveActivityId: ids.join(','),
      pipedriveActivityError: errors.join(' | '),
    },
  });
}
return out;
"""


AFTER_PIPEDRIVE_SYNC_JS = r"""
const row = item.json || {};
let status = row.pipedriveSyncStatus || 'skipped';
if (row.shouldSyncPipedrive) {
  const activities = Array.isArray(row.pipedriveActivities) ? row.pipedriveActivities : [];
  const syncedActivities = activities.filter((activity) => activity.id);
  if (row.pipedriveLeadId && activities.length === 3 && syncedActivities.length === 3) {
    status = activities.some((activity) => activity.status === 'updated')
      ? 'synced_cadence_existing_activities_updated'
      : 'synced_cadence';
  } else if (row.pipedriveLeadId) {
    status = `lead_synced_${syncedActivities.length}_of_3_activities`;
  } else {
    status = 'error';
  }
}
return {
  json: {
    ...row,
    pipedriveSyncStatus: status,
  }
};
"""


AFTER_PIPEDRIVE_AIRTABLE_UPDATE_JS = r"""
const source = $('After Pipedrive Sync').item.json;
return {
  json: {
    ...source,
    airtablePipedriveUpdateResponse: item.json || {},
  }
};
"""


RESPOND_JS = r"""
function allFrom(nodeName) {
  try {
    return $(nodeName).all().map((entry) => entry.json);
  } catch (error) {
    return [];
  }
}

const leadRows = allFrom('Parse Research Result');
const contactRows = allFrom('After Pipedrive Airtable Update').filter((row) => !row.noQualifiedContacts);
const skippedRows = allFrom('Materialize Contact Rows').filter((row) => row.noQualifiedContacts);
const leadIds = new Set(leadRows.map((row) => row.sourceRecordId || row.leadAirtableRecordId || row.brand_name).filter(Boolean));
return [{
  json: {
    ok: true,
    processedLeads: leadIds.size,
    processedContacts: contactRows.length,
    skippedLeads: skippedRows.map((row) => ({
      company: row.companyName,
      domain: row.domain,
      reason: row.skippedReason,
      sales: {
        fullName: row.personaResults?.sales?.fullName || '',
        jobTitle: row.personaResults?.sales?.jobTitle || '',
        confidence: row.personaResults?.sales?.confidence || '',
        sourceUrl: row.personaResults?.sales?.sourceUrl || '',
      },
      ops: {
        fullName: row.personaResults?.ops?.fullName || '',
        jobTitle: row.personaResults?.ops?.jobTitle || '',
        confidence: row.personaResults?.ops?.confidence || '',
        sourceUrl: row.personaResults?.ops?.sourceUrl || '',
      },
      cs: {
        fullName: row.personaResults?.cs?.fullName || '',
        jobTitle: row.personaResults?.cs?.jobTitle || '',
        confidence: row.personaResults?.cs?.confidence || '',
        sourceUrl: row.personaResults?.cs?.sourceUrl || '',
      },
    })),
    contacts: contactRows.map((row) => ({
      persona: row.persona,
      company: row.companyName,
      fullName: row.fullName,
      finalProvider: row.finalProvider,
      finalWorkEmail: row.finalWorkEmail,
      finalEmailValidationStatus: row.finalEmailValidationStatus || '',
      linkedinUrl: row.linkedinUrl || '',
      cadenceEnrollmentDate: row.cadenceEnrollmentDate || '',
      smartleadCampaignId: row.smartleadCampaignId || '',
      smartleadAttempted: Boolean(row.smartleadRaw),
      apifyAttempted: Boolean(row.apifyRaw),
      linkedinActive: row.linkedinActive,
      pipedriveSyncStatus: row.pipedriveSyncStatus,
      pipedriveSkipReason: row.pipedriveSkipReason,
      pipedriveRowIdentity: row.pipedriveRowIdentity,
      pipedriveOwnerName: row.pipedriveOwnerName,
      pipedriveOwnerId: row.pipedriveOwnerId,
      pipedriveSourceChannelId: row.pipedriveSourceChannelId,
      pipedriveTradeshowOptionId: row.pipedriveTradeshowOptionId,
      pipedriveLeadId: row.pipedriveLeadId,
      pipedriveActivityId: row.pipedriveActivityId,
      pipedriveActivities: row.pipedriveActivities || [],
      pipedriveActivityError: row.pipedriveActivityError,
    })),
  }
}];
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Write the workflow JSON to n8n/cultivate_airtable_replacement_loop.json.")
    parser.add_argument("--deploy", action="store_true", help="Create or update the workflow in n8n.")
    parser.add_argument("--activate", action="store_true", help="Activate the workflow after deploy.")
    return parser.parse_args()


def credential_ref(credentials: dict[str, dict[str, str]], key: str) -> tuple[str, dict[str, Any]]:
    name = REQUIRED_CREDENTIALS[key]
    credential = credentials.get(name)
    if not credential:
        raise RuntimeError(f"Missing n8n credential: {name}")
    credential_type = credential["type"]
    return credential_type, {
        credential_type: {
            "id": credential["id"],
            "name": name,
        }
    }


def node(
    name: str,
    type_name: str,
    position: tuple[int, int],
    parameters: dict[str, Any] | None = None,
    *,
    type_version: float | int = 1,
    credentials: dict[str, Any] | None = None,
    disabled: bool | None = None,
    notes: str | None = None,
    continue_on_fail: bool = False,
    always_output_data: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "parameters": parameters or {},
        "id": str(uuid.uuid4()),
        "name": name,
        "type": type_name,
        "typeVersion": type_version,
        "position": [position[0], position[1]],
    }
    if credentials:
        payload["credentials"] = credentials
    if disabled is not None:
        payload["disabled"] = disabled
    if notes:
        payload["notes"] = notes
    if continue_on_fail:
        payload["continueOnFail"] = True
    if always_output_data:
        payload["alwaysOutputData"] = True
    return payload


def code_node(name: str, position: tuple[int, int], js_code: str, *, mode: str = "runOnceForEachItem") -> dict[str, Any]:
    return node(
        name,
        "n8n-nodes-base.code",
        position,
        {"mode": mode, "jsCode": js_code.strip()},
        type_version=2,
    )


def if_node(name: str, position: tuple[int, int], expression: str) -> dict[str, Any]:
    return node(
        name,
        "n8n-nodes-base.if",
        position,
        {"conditions": {"boolean": [{"value1": expression, "value2": True}]}},
        type_version=1,
    )


def merge_node(name: str, position: tuple[int, int]) -> dict[str, Any]:
    return node(name, "n8n-nodes-base.merge", position, {"mode": "append"}, type_version=2)


def http_node(
    name: str,
    position: tuple[int, int],
    *,
    method: str,
    url: str,
    credential_type: str,
    credentials: dict[str, Any],
    json_body: str | None = None,
    query_parameters_json: str | None = None,
    options: dict[str, Any] | None = None,
    continue_on_fail: bool = True,
    always_output_data: bool = False,
) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "method": method,
        "url": url,
        "authentication": "genericCredentialType",
        "genericAuthType": credential_type,
        "options": {"timeout": 120000, **(options or {})},
    }
    if json_body is not None:
        parameters.update({"sendBody": True, "specifyBody": "json", "jsonBody": json_body})
    if query_parameters_json is not None:
        parameters.update(
            {
                "sendQuery": True,
                "specifyQuery": "json",
                "jsonQuery": query_parameters_json,
            }
        )
    return node(
        name,
        "n8n-nodes-base.httpRequest",
        position,
        parameters,
        type_version=4.2,
        credentials=credentials,
        continue_on_fail=continue_on_fail,
        always_output_data=always_output_data,
    )


def connect(connections: dict[str, Any], source: str, target: str, *, source_output: int = 0, target_input: int = 0) -> None:
    source_entry = connections.setdefault(source, {"main": []})
    while len(source_entry["main"]) <= source_output:
        source_entry["main"].append([])
    source_entry["main"][source_output].append({"node": target, "type": "main", "index": target_input})


def fetch_credentials(base_url: str, api_key: str) -> dict[str, dict[str, str]]:
    headers = {"X-N8N-API-KEY": api_key}
    with httpx.Client(timeout=30.0, trust_env=False) as client:
        response = client.get(f"{base_url}/api/v1/credentials", headers=headers)
        response.raise_for_status()
        return {
            item["name"]: {"id": item["id"], "type": item["type"]}
            for item in response.json().get("data", [])
            if isinstance(item, dict) and item.get("name") and item.get("id") and item.get("type")
        }


def build_workflow(credentials_by_name: dict[str, dict[str, str]]) -> dict[str, Any]:
    airtable_type, airtable_credentials = credential_ref(credentials_by_name, "airtable")
    openai_type, openai_credentials = credential_ref(credentials_by_name, "openai")
    leadmagic_type, leadmagic_credentials = credential_ref(credentials_by_name, "leadmagic")
    enrichley_type, enrichley_credentials = credential_ref(credentials_by_name, "enrichley")
    icypeas_type, icypeas_credentials = credential_ref(credentials_by_name, "icypeas")
    prospeo_type, prospeo_credentials = credential_ref(credentials_by_name, "prospeo")
    findymail_type, findymail_credentials = credential_ref(credentials_by_name, "findymail")
    smartlead_type, smartlead_credentials = credential_ref(credentials_by_name, "smartlead")
    apify_type, apify_credentials = credential_ref(credentials_by_name, "apify")
    pipedrive_type, pipedrive_credentials = credential_ref(credentials_by_name, "pipedrive")

    nodes = [
        node("Manual Trigger", "n8n-nodes-base.manualTrigger", (-1300, 0), {}),
        node(
            "Cultivate Intake Webhook",
            "n8n-nodes-base.webhook",
            (-1300, 240),
            {
                "httpMethod": "POST",
                "path": "cultivate-airtable-loop",
                "responseMode": "onReceived",
                "options": {},
            },
            type_version=2,
        ),
        node(
            "Workflow Notes",
            "n8n-nodes-base.stickyNote",
            (-1320, -360),
            {
                "content": (
                    "Cultivate Airtable replacement loop. This workflow does not read Clay. "
                    "It writes the four Cultivate Airtable mirror tables, skips Wiza until a token is added, "
                    "uses only the audited Cultivate providers, stops the email waterfall after the first valid provider, "
                    "and runs Apify only when a verified LinkedIn profile is present."
                ),
                "height": 260,
                "width": 560,
                "color": 5,
            },
            type_version=1,
        ),
        http_node(
            "List Queued Cultivate Leads",
            (-1040, -120),
            method="GET",
            url=f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLES['leads']}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            query_parameters_json=(
                '={{ { "maxRecords": "1000", "pageSize": "100", '
                '"filterByFormula": "AND(LEN({brand_name})>0,LEN({Official Company Domain (2)})=0)" } }}'
            ),
            options={
                "pagination": {
                    "pagination": {
                        "paginationMode": "updateAParameterInEachRequest",
                        "parameters": {
                            "parameters": [
                                {
                                    "type": "qs",
                                    "name": "offset",
                                    "value": "={{$response.body.offset}}",
                                }
                            ]
                        },
                        "paginationCompleteWhen": "other",
                        "completeExpression": "={{!$response.body.offset}}",
                        "limitPagesFetched": True,
                        "maxRequests": 10,
                        "requestInterval": 250,
                    }
                }
            },
        ),
        code_node("Normalize Incoming Leads", (-1040, 120), NORMALIZE_INCOMING_LEADS_JS, mode="runOnceForAllItems"),
        code_node("Build Lead Research Request", (-760, 120), BUILD_RESEARCH_REQUEST_JS),
        http_node(
            "OpenAI Lead and Persona Research",
            (-500, 120),
            method="POST",
            url="https://api.openai.com/v1/responses",
            credential_type=openai_type,
            credentials=openai_credentials,
            json_body="={{$json.openaiResearchRequest}}",
        ),
        code_node("Parse Research Result", (-240, 120), PARSE_RESEARCH_RESULT_JS),
        http_node(
            "Write Cultivate Lead",
            (20, 120),
            method="PATCH",
            url=f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLES['leads']}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableLeadBody}}",
        ),
        code_node("Materialize Contact Rows", (280, 120), MATERIALIZE_CONTACTS_JS, mode="runOnceForAllItems"),
        if_node("Has Qualified Contact?", (540, 120), "={{!$json.noQualifiedContacts}}"),
        http_node(
            "Upsert Cultivate Contact",
            (800, 120),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableContactBody}}",
        ),
        code_node("After Contact Write", (1060, 120), AFTER_CONTACT_WRITE_JS),
        code_node("Build LinkedIn Profile Lookup Request", (1320, -160), BUILD_LINKEDIN_LOOKUP_REQUEST_JS),
        http_node(
            "OpenAI LinkedIn Profile Lookup",
            (1580, -160),
            method="POST",
            url="https://api.openai.com/v1/responses",
            credential_type=openai_type,
            credentials=openai_credentials,
            json_body="={{$json.openaiLinkedinLookupRequest}}",
        ),
        code_node("Parse LinkedIn Profile Lookup", (1840, -160), PARSE_LINKEDIN_LOOKUP_JS),
        http_node(
            "Update Contact LinkedIn Fields",
            (2100, -160),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}/{{{{$json.airtableContactRecordId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableLinkedInUpdateBody}}",
        ),
        code_node("After LinkedIn Airtable Update", (2360, -160), AFTER_LINKEDIN_AIRTABLE_UPDATE_JS),
        code_node("Prepare Email Waterfall", (1320, 120), PREPARE_EMAIL_WATERFALL_JS),
        if_node("Need LeadMagic?", (1580, 120), "={{$json.shouldRunLeadMagic}}"),
        http_node(
            "LeadMagic Email Finder",
            (1840, 40),
            method="POST",
            url="https://api.leadmagic.io/v1/people/email-finder",
            credential_type=leadmagic_type,
            credentials=leadmagic_credentials,
            json_body='={{ { "full_name": $json.fullName, "domain": $json.domain, "company_name": $json.companyName } }}',
        ),
        code_node("Parse LeadMagic", (2100, 40), PARSE_LEADMAGIC_JS),
        merge_node("Merge After LeadMagic", (2360, 120)),
        if_node("Need LeadMagic Validation?", (2620, 120), "={{$json.shouldValidateLeadMagic}}"),
        http_node(
            "Enrichley Validate LeadMagic",
            (2620, 40),
            method="POST",
            url="https://api.enrichley.io/api/v1/validate-single-email",
            credential_type=enrichley_type,
            credentials=enrichley_credentials,
            json_body='={{ { "email": $json.leadMagicEmail } }}',
        ),
        code_node("Parse LeadMagic Validation", (2880, 40), PARSE_LEADMAGIC_VALIDATION_JS),
        merge_node("Merge After LeadMagic Validation", (3140, 120)),
        code_node("After LeadMagic Validation", (3400, 120), AFTER_LEADMAGIC_VALIDATION_JS),
        if_node("Need Icypeas?", (3660, 120), "={{$json.shouldRunIcypeas}}"),
        http_node(
            "Icypeas Start Email Search",
            (3920, 40),
            method="POST",
            url="https://app.icypeas.com/api/email-search",
            credential_type=icypeas_type,
            credentials=icypeas_credentials,
            json_body='={{ { "firstname": $json.firstName, "lastname": $json.lastName, "domainOrCompany": $json.domain } }}',
        ),
        code_node("Parse Icypeas Start", (4180, 40), PARSE_ICYPEAS_START_JS),
        node(
            "Wait for Icypeas",
            "n8n-nodes-base.wait",
            (4440, 40),
            {"resume": "timeInterval", "amount": 8, "unit": "seconds"},
            type_version=1,
        ),
        http_node(
            "Icypeas Read Search",
            (4700, 40),
            method="POST",
            url="https://app.icypeas.com/api/bulk-single-searchs/read",
            credential_type=icypeas_type,
            credentials=icypeas_credentials,
            json_body='={{ { "id": $json.icypeasSearchId } }}',
        ),
        code_node("Parse Icypeas Read", (4960, 40), PARSE_ICYPEAS_READ_JS),
        merge_node("Merge After Icypeas", (5220, 120)),
        code_node("After Icypeas", (5480, 120), AFTER_ICYPEAS_JS),
        if_node("Need Prospeo?", (5740, 120), "={{$json.shouldRunProspeo}}"),
        http_node(
            "Prospeo Enrich Person",
            (6000, 40),
            method="POST",
            url="https://api.prospeo.io/enrich-person",
            credential_type=prospeo_type,
            credentials=prospeo_credentials,
            json_body="={{$json.prospeoRequest}}",
        ),
        code_node("Parse Prospeo", (6260, 40), PARSE_PROSPEO_JS),
        merge_node("Merge After Prospeo", (6520, 120)),
        code_node("After Prospeo", (6780, 120), AFTER_PROSPEO_JS),
        if_node("Need Findymail?", (7040, 120), "={{$json.shouldRunFindymail}}"),
        http_node(
            "Findymail Name Search",
            (7300, 40),
            method="POST",
            url="https://app.findymail.com/api/search/name",
            credential_type=findymail_type,
            credentials=findymail_credentials,
            json_body='={{ { "name": $json.fullName, "domain": $json.domain } }}',
        ),
        code_node("Parse Findymail", (7560, 40), PARSE_FINDYMAIL_JS),
        merge_node("Merge After Findymail", (7820, 120)),
        code_node("Merge Final Email", (8080, 120), MERGE_FINAL_EMAIL_JS),
        if_node("Need Final Email Validation?", (8340, -160), "={{$json.shouldValidateFinalEmail}}"),
        http_node(
            "Enrichley Validate Final Email",
            (8600, -240),
            method="POST",
            url="https://api.enrichley.io/api/v1/validate-single-email",
            credential_type=enrichley_type,
            credentials=enrichley_credentials,
            json_body='={{ { "email": $json.finalWorkEmail } }}',
        ),
        code_node("Parse Final Email Validation", (8860, -240), PARSE_FINAL_EMAIL_VALIDATION_JS),
        merge_node("Merge After Final Email Validation", (9120, -160)),
        code_node("Apply Final Email Validation", (9380, -160), APPLY_FINAL_EMAIL_VALIDATION_JS),
        http_node(
            "Update Contact Email Fields",
            (8340, 120),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}/{{{{$json.airtableContactRecordId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableContactUpdateBody}}",
        ),
        code_node("After Email Airtable Update", (8600, 120), AFTER_EMAIL_AIRTABLE_UPDATE_JS),
        if_node("Need Smartlead?", (8860, 120), "={{$json.shouldPushSmartlead}}"),
        http_node(
            "Add Lead to Smartlead",
            (9120, 40),
            method="POST",
            url="=https://server.smartlead.ai/api/v1/campaigns/{{$json.smartleadCampaignId}}/leads",
            credential_type=smartlead_type,
            credentials=smartlead_credentials,
            json_body=(
                '={{ { "lead_list": [{ "email": $json.finalWorkEmail, "first_name": $json.firstName, '
                '"company_name": $json.normalizedName || $json.companyName, "custom_fields": { '
                '"persona": $json.persona, "show_name": $json.conference, "source_lead_record_id": $json.sourceLeadRecordId, '
                '"airtable_contact_record_id": $json.airtableContactRecordId } }], '
                '"settings": { "ignore_duplicate_leads_in_other_campaign": false, '
                '"ignore_global_block_list": false, "ignore_unsubscribe_list": false, '
                '"ignore_community_bounce_list": false, "return_lead_ids": true } } }}'
            ),
        ),
        code_node("Parse Smartlead", (9380, 40), PARSE_SMARTLEAD_JS),
        merge_node("Merge After Smartlead", (9640, 120)),
        code_node("After Smartlead", (9900, 120), AFTER_SMARTLEAD_JS),
        http_node(
            "Update Contact Smartlead Fields",
            (10160, 120),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}/{{{{$json.airtableContactRecordId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableSmartleadUpdateBody}}",
        ),
        code_node("After Smartlead Airtable Update", (10420, 120), AFTER_SMARTLEAD_AIRTABLE_UPDATE_JS),
        if_node("Need Apify?", (10680, 120), "={{$json.shouldRunApify}}"),
        http_node(
            "Run Apify LinkedIn Actor",
            (10940, 40),
            method="POST",
            url=f"https://api.apify.com/v2/acts/{APIFY_LINKEDIN_ACTOR_ID}/run-sync-get-dataset-items",
            credential_type=apify_type,
            credentials=apify_credentials,
            json_body='={{ { "maxItems": 1, "profiles": [$json.linkedinUrl] } }}',
            options={
                "response": {
                    "response": {
                        "responseFormat": "text",
                        "outputPropertyName": "apifyResponseText",
                    }
                }
            },
            always_output_data=True,
        ),
        code_node("Parse Apify", (11200, 40), PARSE_APIFY_JS),
        merge_node("Merge After Apify", (11460, 120)),
        code_node("After Apify", (11720, 120), AFTER_APIFY_JS),
        http_node(
            "Update Contact Apify Fields",
            (11980, 120),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}/{{{{$json.airtableContactRecordId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body="={{$json.airtableApifyUpdateBody}}",
        ),
        code_node("After Apify Airtable Update", (12240, 120), AFTER_APIFY_AIRTABLE_UPDATE_JS),
        code_node("Build Pipedrive Sync", (12500, 120), BUILD_PIPEDRIVE_SYNC_JS),
        if_node("Need Pipedrive Sync?", (12760, 120), "={{$json.shouldSyncPipedrive}}"),
        http_node(
            "Pipedrive Search Organization",
            (13020, 40),
            method="GET",
            url="https://api.pipedrive.com/v1/organizations/search",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            query_parameters_json='={{ JSON.stringify({ "term": $json.pipedriveOrgSearchTerm, "fields": "name", "limit": "10" }) }}',
        ),
        code_node("Parse Pipedrive Org Search", (13280, 40), PARSE_PIPEDRIVE_ORG_SEARCH_JS),
        if_node("Need Create Pipedrive Org?", (13540, 40), "={{!$json.pipedriveOrgId}}"),
        http_node(
            "Pipedrive Create Organization",
            (13800, -40),
            method="POST",
            url="https://api.pipedrive.com/v1/organizations",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body='={{ { "name": $json.companyName, "owner_id": $json.pipedriveOwnerId, "visible_to": "3" } }}',
        ),
        http_node(
            "Pipedrive Update Organization",
            (13800, 120),
            method="PUT",
            url="=https://api.pipedrive.com/v1/organizations/{{$json.pipedriveOrgId}}",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body='={{ { "name": $json.companyName, "owner_id": $json.pipedriveOwnerId, "visible_to": "3" } }}',
        ),
        code_node("Parse Pipedrive Org Create", (14060, -40), PARSE_PIPEDRIVE_ORG_CREATE_JS),
        merge_node("Merge Pipedrive Org", (14320, 40)),
        http_node(
            "Pipedrive Search Person",
            (14580, 40),
            method="GET",
            url="https://api.pipedrive.com/v1/persons/search",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            query_parameters_json='={{ JSON.stringify({ "term": $json.pipedrivePersonSearchTerm, "fields": $json.pipedrivePersonSearchFields, "exact_match": true, "limit": "10" }) }}',
        ),
        code_node("Parse Pipedrive Person Search", (14840, 40), PARSE_PIPEDRIVE_PERSON_SEARCH_JS),
        if_node("Need Create Pipedrive Person?", (15100, 40), "={{!$json.pipedrivePersonId}}"),
        http_node(
            "Pipedrive Create Person",
            (15360, -40),
            method="POST",
            url="https://api.pipedrive.com/v1/persons",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "name": $json.fullName, "owner_id": $json.pipedriveOwnerId, "visible_to": "3" }; '
                'if ($json.pipedriveOrgId) body.org_id = Number($json.pipedriveOrgId); '
                'if ($json.finalWorkEmail) body.email = [{ "value": $json.finalWorkEmail, "primary": true, "label": "work" }]; '
                'if ($json.finalPhone) body.phone = [{ "value": $json.finalPhone, "primary": true, "label": "work" }]; '
                'if ($json.jobTitle) body.job_title = $json.jobTitle; '
                f'if ($json.linkedinUrl) body["{PIPEDRIVE_PERSON_LINKEDIN_PROFILE_FIELD_KEY}"] = $json.linkedinUrl; '
                f'if ($json.linkedinUrl) body["{PIPEDRIVE_PERSON_LINKEDIN_URL_FIELD_KEY}"] = $json.linkedinUrl; '
                'return body; })() }}'
            ),
        ),
        http_node(
            "Pipedrive Update Person",
            (15360, 120),
            method="PUT",
            url="=https://api.pipedrive.com/v1/persons/{{$json.pipedrivePersonId}}",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "name": $json.fullName, "owner_id": $json.pipedriveOwnerId, "visible_to": "3" }; '
                'if ($json.pipedriveOrgId) body.org_id = Number($json.pipedriveOrgId); '
                'if ($json.finalWorkEmail) body.email = [{ "value": $json.finalWorkEmail, "primary": true, "label": "work" }]; '
                'if ($json.finalPhone) body.phone = [{ "value": $json.finalPhone, "primary": true, "label": "work" }]; '
                'if ($json.jobTitle) body.job_title = $json.jobTitle; '
                f'if ($json.linkedinUrl) body["{PIPEDRIVE_PERSON_LINKEDIN_PROFILE_FIELD_KEY}"] = $json.linkedinUrl; '
                f'if ($json.linkedinUrl) body["{PIPEDRIVE_PERSON_LINKEDIN_URL_FIELD_KEY}"] = $json.linkedinUrl; '
                'return body; })() }}'
            ),
        ),
        code_node("Parse Pipedrive Person Create", (15620, -40), PARSE_PIPEDRIVE_PERSON_CREATE_JS),
        merge_node("Merge Pipedrive Person", (15880, 40)),
        http_node(
            "Pipedrive Get Tradeshow Field",
            (16140, -160),
            method="GET",
            url=f"https://api.pipedrive.com/api/v2/dealFields/{PIPEDRIVE_TRADESHOW_FIELD_KEY}",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
        ),
        code_node("Parse Pipedrive Tradeshow Field", (16400, -160), PARSE_PIPEDRIVE_TRADESHOW_FIELD_JS),
        if_node("Need Create Pipedrive Tradeshow Option?", (16660, -160), "={{$json.shouldCreatePipedriveTradeshowOption}}"),
        http_node(
            "Pipedrive Create Tradeshow Option",
            (16920, -240),
            method="POST",
            url=f"https://api.pipedrive.com/api/v2/dealFields/{PIPEDRIVE_TRADESHOW_FIELD_KEY}/options",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body='={{ [{ "label": $json.pipedriveShowContext }] }}',
        ),
        code_node("Parse Pipedrive Tradeshow Option Create", (17180, -240), PARSE_PIPEDRIVE_TRADESHOW_OPTION_CREATE_JS),
        merge_node("Merge Pipedrive Tradeshow Option", (17440, -160)),
        http_node(
            "Pipedrive Search Lead",
            (17680, 40),
            method="GET",
            url="https://api.pipedrive.com/v1/leads",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            query_parameters_json=(
                '={{ (() => { const q = { "limit": "100", "owner_id": String($json.pipedriveOwnerId) }; '
                'if ($json.pipedrivePersonId) q.person_id = String($json.pipedrivePersonId); '
                'if ($json.pipedriveOrgId) q.organization_id = String($json.pipedriveOrgId); '
                'return JSON.stringify(q); })() }}'
            ),
        ),
        code_node("Parse Pipedrive Lead Search", (17940, 40), PARSE_PIPEDRIVE_LEAD_SEARCH_JS),
        if_node("Need Create Pipedrive Lead?", (18200, 40), "={{!$json.pipedriveLeadId}}"),
        http_node(
            "Pipedrive Create Lead",
            (18460, -40),
            method="POST",
            url="https://api.pipedrive.com/v1/leads",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "title": $json.pipedriveLeadTitle, "owner_id": $json.pipedriveOwnerId, '
                '"visible_to": "3", "channel_id": $json.pipedriveSourceChannelId }; '
                'if ($json.pipedriveOriginId) body.origin_id = $json.pipedriveOriginId; '
                'if ($json.pipedrivePersonId) body.person_id = Number($json.pipedrivePersonId); '
                'if ($json.pipedriveOrgId) body.organization_id = Number($json.pipedriveOrgId); '
                f'if ($json.pipedriveTradeshowOptionId) body["{PIPEDRIVE_TRADESHOW_FIELD_KEY}"] = Number($json.pipedriveTradeshowOptionId); '
                f'body["{PIPEDRIVE_INDUSTRY_FIELD_KEY}"] = {PIPEDRIVE_DYNAMIC_EVENT_INDUSTRY_OPTION_ID}; '
                'return body; })() }}'
            ),
        ),
        http_node(
            "Pipedrive Update Lead",
            (18460, 120),
            method="PATCH",
            url="=https://api.pipedrive.com/v1/leads/{{$json.pipedriveLeadId}}",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "title": $json.pipedriveLeadTitle, "owner_id": $json.pipedriveOwnerId, '
                '"visible_to": "3", "channel_id": $json.pipedriveSourceChannelId }; '
                'if ($json.pipedriveOriginId) body.origin_id = $json.pipedriveOriginId; '
                'if ($json.pipedrivePersonId) body.person_id = Number($json.pipedrivePersonId); '
                'if ($json.pipedriveOrgId) body.organization_id = Number($json.pipedriveOrgId); '
                f'if ($json.pipedriveTradeshowOptionId) body["{PIPEDRIVE_TRADESHOW_FIELD_KEY}"] = Number($json.pipedriveTradeshowOptionId); '
                f'body["{PIPEDRIVE_INDUSTRY_FIELD_KEY}"] = {PIPEDRIVE_DYNAMIC_EVENT_INDUSTRY_OPTION_ID}; '
                'return body; })() }}'
            ),
        ),
        code_node("Parse Pipedrive Lead Upsert", (18720, 40), PARSE_PIPEDRIVE_LEAD_UPSERT_JS),
        if_node("Need Pipedrive Activities?", (18980, 40), "={{$json.shouldSyncPipedrive && !!$json.pipedriveLeadId}}"),
        code_node(
            "Materialize Pipedrive Activities",
            (19240, -40),
            MATERIALIZE_PIPEDRIVE_ACTIVITIES_JS,
            mode="runOnceForAllItems",
        ),
        http_node(
            "Pipedrive Search Activity",
            (19500, -40),
            method="GET",
            url="https://api.pipedrive.com/api/v2/activities",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            query_parameters_json='={{ JSON.stringify({ "limit": "100", "owner_id": $json.pipedriveOwnerId, "lead_id": $json.pipedriveLeadId }) }}',
        ),
        code_node("Parse Pipedrive Activity Search", (19760, -40), PARSE_PIPEDRIVE_ACTIVITY_SEARCH_JS),
        if_node("Need Create Pipedrive Activity?", (20020, -40), "={{!$json.pipedriveActivityId}}"),
        http_node(
            "Pipedrive Create Activity",
            (20280, -120),
            method="POST",
            url="https://api.pipedrive.com/api/v2/activities",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "subject": $json.pipedriveActivitySubject, "type": $json.pipedriveActivityType, '
                '"owner_id": $json.pipedriveOwnerId, "due_date": $json.pipedriveActivityDueDate, '
                '"note": $json.pipedriveActivityNote, "done": false }; '
                'if ($json.pipedriveLeadId) body.lead_id = $json.pipedriveLeadId; '
                'if ($json.pipedrivePersonId) body.participants = [{ "person_id": Number($json.pipedrivePersonId), "primary": true }]; '
                'if ($json.pipedriveOrgId) body.org_id = Number($json.pipedriveOrgId); '
                'return body; })() }}'
            ),
        ),
        http_node(
            "Pipedrive Update Activity",
            (20280, 40),
            method="PATCH",
            url="=https://api.pipedrive.com/api/v2/activities/{{$json.pipedriveActivityId}}",
            credential_type=pipedrive_type,
            credentials=pipedrive_credentials,
            json_body=(
                '={{ (() => { const body = { "subject": $json.pipedriveActivitySubject, "type": $json.pipedriveActivityType, '
                '"owner_id": $json.pipedriveOwnerId, "due_date": $json.pipedriveActivityDueDate, '
                '"note": $json.pipedriveActivityNote }; '
                'if ($json.pipedriveLeadId) body.lead_id = $json.pipedriveLeadId; '
                'if ($json.pipedrivePersonId) body.participants = [{ "person_id": Number($json.pipedrivePersonId), "primary": true }]; '
                'if ($json.pipedriveOrgId) body.org_id = Number($json.pipedriveOrgId); '
                'return body; })() }}'
            ),
        ),
        code_node("Parse Pipedrive Activity Create", (20540, -120), PARSE_PIPEDRIVE_ACTIVITY_UPSERT_JS),
        code_node("Parse Pipedrive Activity Update", (20540, 40), PARSE_PIPEDRIVE_ACTIVITY_UPSERT_JS),
        merge_node("Merge Pipedrive Activity Upserts", (20800, -40)),
        code_node(
            "Summarize Pipedrive Activities",
            (21060, -40),
            SUMMARIZE_PIPEDRIVE_ACTIVITIES_JS,
            mode="runOnceForAllItems",
        ),
        code_node("After Pipedrive Sync", (21320, 120), AFTER_PIPEDRIVE_SYNC_JS),
        http_node(
            "Update Contact Pipedrive Fields",
            (21580, 120),
            method="PATCH",
            url=f"=https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{{{{$json.contactAirtableTableId}}}}/{{{{$json.airtableContactRecordId}}}}",
            credential_type=airtable_type,
            credentials=airtable_credentials,
            json_body=(
                '={{ { "fields": { '
                '"Pipedrive Row Identity": $json.pipedriveRowIdentity || "", '
                '"Pipedrive Sync Status": $json.pipedriveSyncStatus || "", '
                '"Pipedrive Skip Reason": $json.pipedriveSkipReason || "", '
                '"Pipedrive Owner Name": $json.pipedriveOwnerName || "", '
                '"Pipedrive Owner ID": String($json.pipedriveOwnerId || ""), '
                '"Pipedrive Source Channel ID": $json.pipedriveSourceChannelId || "", '
                '"Pipedrive Tradeshow Option ID": String($json.pipedriveTradeshowOptionId || ""), '
                '"Pipedrive Org ID": String($json.pipedriveOrgId || ""), '
                '"Pipedrive Person ID": String($json.pipedrivePersonId || ""), '
                '"Pipedrive Lead ID": String($json.pipedriveLeadId || ""), '
                '"Pipedrive Activity ID": String($json.pipedriveActivityId || ""), '
                '"Pipedrive Activity Error": $json.pipedriveActivityError || $json.pipedriveLeadError || $json.pipedrivePersonError || $json.pipedriveOrgError || "", '
                '"Pipedrive Synced At": new Date().toISOString() '
                '}, "typecast": true } }}'
            ),
        ),
        code_node("After Pipedrive Airtable Update", (21840, 120), AFTER_PIPEDRIVE_AIRTABLE_UPDATE_JS),
        merge_node("Merge Response Rows", (22100, 120)),
        code_node("Build Webhook Response", (22360, 120), RESPOND_JS, mode="runOnceForAllItems"),
    ]

    connections: dict[str, Any] = {}
    connect(connections, "Manual Trigger", "List Queued Cultivate Leads")
    connect(connections, "List Queued Cultivate Leads", "Normalize Incoming Leads")
    connect(connections, "Cultivate Intake Webhook", "Normalize Incoming Leads")
    connect(connections, "Normalize Incoming Leads", "Build Lead Research Request")
    connect(connections, "Build Lead Research Request", "OpenAI Lead and Persona Research")
    connect(connections, "OpenAI Lead and Persona Research", "Parse Research Result")
    connect(connections, "Parse Research Result", "Write Cultivate Lead")
    connect(connections, "Write Cultivate Lead", "Materialize Contact Rows")
    connect(connections, "Materialize Contact Rows", "Has Qualified Contact?")
    connect(connections, "Has Qualified Contact?", "Upsert Cultivate Contact", source_output=0)
    connect(connections, "Has Qualified Contact?", "Merge Response Rows", source_output=1, target_input=1)
    connect(connections, "Upsert Cultivate Contact", "After Contact Write")
    connect(connections, "After Contact Write", "Build LinkedIn Profile Lookup Request")
    connect(connections, "Build LinkedIn Profile Lookup Request", "OpenAI LinkedIn Profile Lookup")
    connect(connections, "OpenAI LinkedIn Profile Lookup", "Parse LinkedIn Profile Lookup")
    connect(connections, "Parse LinkedIn Profile Lookup", "Update Contact LinkedIn Fields")
    connect(connections, "Update Contact LinkedIn Fields", "After LinkedIn Airtable Update")
    connect(connections, "After LinkedIn Airtable Update", "Prepare Email Waterfall")
    connect(connections, "Prepare Email Waterfall", "Need LeadMagic?")
    connect(connections, "Need LeadMagic?", "LeadMagic Email Finder", source_output=0)
    connect(connections, "Need LeadMagic?", "Merge After LeadMagic", source_output=1, target_input=1)
    connect(connections, "LeadMagic Email Finder", "Parse LeadMagic")
    connect(connections, "Parse LeadMagic", "Merge After LeadMagic", target_input=0)
    connect(connections, "Merge After LeadMagic", "Need LeadMagic Validation?")
    connect(connections, "Need LeadMagic Validation?", "Enrichley Validate LeadMagic", source_output=0)
    connect(connections, "Need LeadMagic Validation?", "Merge After LeadMagic Validation", source_output=1, target_input=1)
    connect(connections, "Enrichley Validate LeadMagic", "Parse LeadMagic Validation")
    connect(connections, "Parse LeadMagic Validation", "Merge After LeadMagic Validation", target_input=0)
    connect(connections, "Merge After LeadMagic Validation", "After LeadMagic Validation")
    connect(connections, "After LeadMagic Validation", "Need Icypeas?")
    connect(connections, "Need Icypeas?", "Icypeas Start Email Search", source_output=0)
    connect(connections, "Need Icypeas?", "Merge After Icypeas", source_output=1, target_input=1)
    connect(connections, "Icypeas Start Email Search", "Parse Icypeas Start")
    connect(connections, "Parse Icypeas Start", "Wait for Icypeas")
    connect(connections, "Wait for Icypeas", "Icypeas Read Search")
    connect(connections, "Icypeas Read Search", "Parse Icypeas Read")
    connect(connections, "Parse Icypeas Read", "Merge After Icypeas", target_input=0)
    connect(connections, "Merge After Icypeas", "After Icypeas")
    connect(connections, "After Icypeas", "Need Prospeo?")
    connect(connections, "Need Prospeo?", "Prospeo Enrich Person", source_output=0)
    connect(connections, "Need Prospeo?", "Merge After Prospeo", source_output=1, target_input=1)
    connect(connections, "Prospeo Enrich Person", "Parse Prospeo")
    connect(connections, "Parse Prospeo", "Merge After Prospeo", target_input=0)
    connect(connections, "Merge After Prospeo", "After Prospeo")
    connect(connections, "After Prospeo", "Need Findymail?")
    connect(connections, "Need Findymail?", "Findymail Name Search", source_output=0)
    connect(connections, "Need Findymail?", "Merge After Findymail", source_output=1, target_input=1)
    connect(connections, "Findymail Name Search", "Parse Findymail")
    connect(connections, "Parse Findymail", "Merge After Findymail", target_input=0)
    connect(connections, "Merge After Findymail", "Merge Final Email")
    connect(connections, "Merge Final Email", "Need Final Email Validation?")
    connect(connections, "Need Final Email Validation?", "Enrichley Validate Final Email", source_output=0)
    connect(connections, "Need Final Email Validation?", "Merge After Final Email Validation", source_output=1, target_input=1)
    connect(connections, "Enrichley Validate Final Email", "Parse Final Email Validation")
    connect(connections, "Parse Final Email Validation", "Merge After Final Email Validation", target_input=0)
    connect(connections, "Merge After Final Email Validation", "Apply Final Email Validation")
    connect(connections, "Apply Final Email Validation", "Update Contact Email Fields")
    connect(connections, "Update Contact Email Fields", "After Email Airtable Update")
    connect(connections, "After Email Airtable Update", "Need Smartlead?")
    connect(connections, "Need Smartlead?", "Add Lead to Smartlead", source_output=0)
    connect(connections, "Need Smartlead?", "Merge After Smartlead", source_output=1, target_input=1)
    connect(connections, "Add Lead to Smartlead", "Parse Smartlead")
    connect(connections, "Parse Smartlead", "Merge After Smartlead", target_input=0)
    connect(connections, "Merge After Smartlead", "After Smartlead")
    connect(connections, "After Smartlead", "Update Contact Smartlead Fields")
    connect(connections, "Update Contact Smartlead Fields", "After Smartlead Airtable Update")
    connect(connections, "After Smartlead Airtable Update", "Need Apify?")
    connect(connections, "Need Apify?", "Run Apify LinkedIn Actor", source_output=0)
    connect(connections, "Need Apify?", "After Apify", source_output=1)
    connect(connections, "Run Apify LinkedIn Actor", "Parse Apify")
    connect(connections, "Parse Apify", "After Apify")
    connect(connections, "After Apify", "Update Contact Apify Fields")
    connect(connections, "Update Contact Apify Fields", "After Apify Airtable Update")
    connect(connections, "After Apify Airtable Update", "Build Pipedrive Sync")
    connect(connections, "Build Pipedrive Sync", "Need Pipedrive Sync?")
    connect(connections, "Need Pipedrive Sync?", "Pipedrive Search Organization", source_output=0)
    connect(connections, "Need Pipedrive Sync?", "After Pipedrive Sync", source_output=1)
    connect(connections, "Pipedrive Search Organization", "Parse Pipedrive Org Search")
    connect(connections, "Parse Pipedrive Org Search", "Need Create Pipedrive Org?")
    connect(connections, "Need Create Pipedrive Org?", "Pipedrive Create Organization", source_output=0)
    connect(connections, "Need Create Pipedrive Org?", "Pipedrive Update Organization", source_output=1)
    connect(connections, "Pipedrive Create Organization", "Parse Pipedrive Org Create")
    connect(connections, "Pipedrive Update Organization", "Parse Pipedrive Org Create")
    connect(connections, "Parse Pipedrive Org Create", "Merge Pipedrive Org", target_input=0)
    connect(connections, "Merge Pipedrive Org", "Pipedrive Search Person")
    connect(connections, "Pipedrive Search Person", "Parse Pipedrive Person Search")
    connect(connections, "Parse Pipedrive Person Search", "Need Create Pipedrive Person?")
    connect(connections, "Need Create Pipedrive Person?", "Pipedrive Create Person", source_output=0)
    connect(connections, "Need Create Pipedrive Person?", "Pipedrive Update Person", source_output=1)
    connect(connections, "Pipedrive Create Person", "Parse Pipedrive Person Create")
    connect(connections, "Pipedrive Update Person", "Parse Pipedrive Person Create")
    connect(connections, "Parse Pipedrive Person Create", "Merge Pipedrive Person", target_input=0)
    connect(connections, "Merge Pipedrive Person", "Pipedrive Get Tradeshow Field")
    connect(connections, "Pipedrive Get Tradeshow Field", "Parse Pipedrive Tradeshow Field")
    connect(connections, "Parse Pipedrive Tradeshow Field", "Need Create Pipedrive Tradeshow Option?")
    connect(connections, "Need Create Pipedrive Tradeshow Option?", "Pipedrive Create Tradeshow Option", source_output=0)
    connect(connections, "Need Create Pipedrive Tradeshow Option?", "Merge Pipedrive Tradeshow Option", source_output=1, target_input=1)
    connect(connections, "Pipedrive Create Tradeshow Option", "Parse Pipedrive Tradeshow Option Create")
    connect(connections, "Parse Pipedrive Tradeshow Option Create", "Merge Pipedrive Tradeshow Option", target_input=0)
    connect(connections, "Merge Pipedrive Tradeshow Option", "Pipedrive Search Lead")
    connect(connections, "Pipedrive Search Lead", "Parse Pipedrive Lead Search")
    connect(connections, "Parse Pipedrive Lead Search", "Need Create Pipedrive Lead?")
    connect(connections, "Need Create Pipedrive Lead?", "Pipedrive Create Lead", source_output=0)
    connect(connections, "Need Create Pipedrive Lead?", "Pipedrive Update Lead", source_output=1)
    connect(connections, "Pipedrive Create Lead", "Parse Pipedrive Lead Upsert")
    connect(connections, "Pipedrive Update Lead", "Parse Pipedrive Lead Upsert")
    connect(connections, "Parse Pipedrive Lead Upsert", "Need Pipedrive Activities?")
    connect(connections, "Need Pipedrive Activities?", "Materialize Pipedrive Activities", source_output=0)
    connect(connections, "Need Pipedrive Activities?", "After Pipedrive Sync", source_output=1)
    connect(connections, "Materialize Pipedrive Activities", "Pipedrive Search Activity")
    connect(connections, "Pipedrive Search Activity", "Parse Pipedrive Activity Search")
    connect(connections, "Parse Pipedrive Activity Search", "Need Create Pipedrive Activity?")
    connect(connections, "Need Create Pipedrive Activity?", "Pipedrive Create Activity", source_output=0)
    connect(connections, "Need Create Pipedrive Activity?", "Pipedrive Update Activity", source_output=1)
    connect(connections, "Pipedrive Create Activity", "Parse Pipedrive Activity Create")
    connect(connections, "Pipedrive Update Activity", "Parse Pipedrive Activity Update")
    connect(connections, "Parse Pipedrive Activity Create", "Merge Pipedrive Activity Upserts", target_input=0)
    connect(connections, "Parse Pipedrive Activity Update", "Merge Pipedrive Activity Upserts", target_input=1)
    connect(connections, "Merge Pipedrive Activity Upserts", "Summarize Pipedrive Activities")
    connect(connections, "Summarize Pipedrive Activities", "After Pipedrive Sync")
    connect(connections, "After Pipedrive Sync", "Update Contact Pipedrive Fields")
    connect(connections, "Update Contact Pipedrive Fields", "After Pipedrive Airtable Update")
    connect(connections, "After Pipedrive Airtable Update", "Merge Response Rows", target_input=0)
    connect(connections, "Merge Response Rows", "Build Webhook Response")

    return {
        "name": WORKFLOW_NAME,
        "active": False,
        "nodes": nodes,
        "connections": connections,
        "settings": {"executionOrder": "v1"},
        "staticData": None,
        "pinData": {},
        "meta": {"templateCredsSetupCompleted": True},
        "tags": [],
    }


def deploy_workflow(base_url: str, api_key: str, workflow: dict[str, Any], *, activate: bool) -> dict[str, Any]:
    headers = {"X-N8N-API-KEY": api_key, "Content-Type": "application/json"}
    with httpx.Client(timeout=60.0, trust_env=False) as client:
        existing_response = client.get(f"{base_url}/api/v1/workflows", headers=headers)
        existing_response.raise_for_status()
        existing = next(
            (
                item
                for item in existing_response.json().get("data", [])
                if item.get("name") == WORKFLOW_NAME
            ),
            None,
        )
        payload = {
            "name": workflow["name"],
            "nodes": workflow["nodes"],
            "connections": workflow["connections"],
            "settings": workflow["settings"],
        }
        if existing:
            workflow_id = existing["id"]
            response = client.put(f"{base_url}/api/v1/workflows/{workflow_id}", headers=headers, json=payload)
            action = "updated"
        else:
            response = client.post(f"{base_url}/api/v1/workflows", headers=headers, json=payload)
            action = "created"
        response.raise_for_status()
        data = response.json()
        workflow_id = data.get("id") or (existing or {}).get("id")

        active = bool(data.get("active"))
        if activate and workflow_id and not active:
            activation = client.post(f"{base_url}/api/v1/workflows/{workflow_id}/activate", headers=headers)
            activation.raise_for_status()
            active = True

        return {"status": action, "id": workflow_id, "name": WORKFLOW_NAME, "active": active}


def main() -> int:
    load_dotenv(ROOT / ".env")
    args = parse_args()
    if not args.write and not args.deploy:
        args.write = True

    base_url = os.getenv(N8N_BASE_ENV, "").rstrip("/")
    api_key = os.getenv(N8N_API_ENV, "")
    if not base_url or not api_key:
        raise SystemExit(f"Missing {N8N_BASE_ENV} or {N8N_API_ENV}.")

    credentials = fetch_credentials(base_url, api_key)
    workflow = build_workflow(credentials)

    result: dict[str, Any] = {
        "workflow_name": WORKFLOW_NAME,
        "workflow_path": str(WORKFLOW_PATH),
        "node_count": len(workflow["nodes"]),
        "uses_clay_api": False,
        "uses_only_audited_cultivate_providers": True,
        "wiza_mode": "placeholder_skipped",
    }

    if args.write:
        WORKFLOW_PATH.parent.mkdir(parents=True, exist_ok=True)
        WORKFLOW_PATH.write_text(json.dumps(workflow, indent=2) + "\n", encoding="utf-8")
        result["written"] = True

    if args.deploy:
        result["deploy"] = deploy_workflow(base_url, api_key, workflow, activate=args.activate)

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

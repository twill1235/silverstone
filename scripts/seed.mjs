// scripts/seed.mjs
// Produces a realistic-looking seed dataset so the site renders immediately
// before the first live Redfin pull runs. Replaced on first /api/refresh.

import fs from "node:fs";
import path from "node:path";

const STATES = [
  ["AL", "Alabama", 5108468], ["AK", "Alaska", 733406], ["AZ", "Arizona", 7431344],
  ["AR", "Arkansas", 3067732], ["CA", "California", 38965193], ["CO", "Colorado", 5877610],
  ["CT", "Connecticut", 3617176], ["DE", "Delaware", 1031890], ["FL", "Florida", 22610726],
  ["GA", "Georgia", 11029227], ["HI", "Hawaii", 1435138], ["ID", "Idaho", 1964726],
  ["IL", "Illinois", 12549689], ["IN", "Indiana", 6862199], ["IA", "Iowa", 3207004],
  ["KS", "Kansas", 2940546], ["KY", "Kentucky", 4526154], ["LA", "Louisiana", 4573749],
  ["ME", "Maine", 1395722], ["MD", "Maryland", 6180253], ["MA", "Massachusetts", 7001399],
  ["MI", "Michigan", 10037261], ["MN", "Minnesota", 5737915], ["MS", "Mississippi", 2939690],
  ["MO", "Missouri", 6196156], ["MT", "Montana", 1132812], ["NE", "Nebraska", 1978379],
  ["NV", "Nevada", 3194176], ["NH", "New Hampshire", 1402054], ["NJ", "New Jersey", 9290841],
  ["NM", "New Mexico", 2114371], ["NY", "New York", 19571216], ["NC", "North Carolina", 10835491],
  ["ND", "North Dakota", 783926], ["OH", "Ohio", 11785935], ["OK", "Oklahoma", 4053824],
  ["OR", "Oregon", 4233358], ["PA", "Pennsylvania", 12961683], ["RI", "Rhode Island", 1095962],
  ["SC", "South Carolina", 5373555], ["SD", "South Dakota", 919318], ["TN", "Tennessee", 7126489],
  ["TX", "Texas", 30503301], ["UT", "Utah", 3417734], ["VT", "Vermont", 647464],
  ["VA", "Virginia", 8715698], ["WA", "Washington", 7812880], ["WV", "West Virginia", 1770071],
  ["WI", "Wisconsin", 5910955], ["WY", "Wyoming", 584057], ["DC", "District of Columbia", 678972]
];

const COUNTIES = [
  // [name, state_code, population, median_price_k, vol_bucket]
  ["Maricopa County", "AZ", 4585871, 465, "high"],
  ["Los Angeles County", "CA", 9721138, 865, "high"],
  ["San Diego County", "CA", 3286069, 925, "high"],
  ["Orange County", "CA", 3186989, 1125, "high"],
  ["Santa Clara County", "CA", 1870945, 1650, "med"],
  ["Alameda County", "CA", 1649060, 1100, "med"],
  ["Denver County", "CO", 710018, 585, "med"],
  ["Miami-Dade County", "FL", 2673837, 625, "high"],
  ["Broward County", "FL", 1944375, 525, "high"],
  ["Palm Beach County", "FL", 1518475, 595, "high"],
  ["Orange County", "FL", 1452726, 435, "high"],
  ["Hillsborough County", "FL", 1513301, 385, "high"],
  ["Fulton County", "GA", 1066710, 435, "high"],
  ["Cook County", "IL", 5109292, 325, "high"],
  ["Middlesex County", "MA", 1632002, 745, "med"],
  ["Wake County", "NC", 1165527, 465, "high"],
  ["Mecklenburg County", "NC", 1148824, 415, "high"],
  ["Clark County", "NV", 2336573, 445, "high"],
  ["Kings County", "NY", 2590516, 895, "high"],
  ["Queens County", "NY", 2252196, 725, "high"],
  ["Nassau County", "NY", 1389927, 745, "med"],
  ["Franklin County", "OH", 1326063, 285, "med"],
  ["Harris County", "TX", 4835125, 335, "high"],
  ["Dallas County", "TX", 2606356, 365, "high"],
  ["Tarrant County", "TX", 2182947, 335, "high"],
  ["Travis County", "TX", 1326436, 545, "med"],
  ["Bexar County", "TX", 2009324, 285, "high"],
  ["Fairfax County", "VA", 1139257, 745, "med"],
  ["Loudoun County", "VA", 429939, 735, "med"],
  ["King County", "WA", 2269675, 845, "high"],
  ["Salt Lake County", "UT", 1199891, 565, "med"],
  ["Multnomah County", "OR", 795083, 525, "med"],
  ["Hennepin County", "MN", 1281565, 365, "med"],
  ["Jefferson County", "KY", 782969, 245, "med"],
  ["Davidson County", "TN", 715884, 475, "med"],
  ["Shelby County", "TN", 910530, 225, "med"],
  ["Oklahoma County", "OK", 802585, 235, "med"],
  ["Marion County", "IN", 977203, 235, "med"],
  ["Providence County", "RI", 660741, 425, "med"],
  ["Hartford County", "CT", 899498, 335, "med"],
  ["New Haven County", "CT", 864835, 375, "med"],
  ["Essex County", "NJ", 863628, 495, "med"],
  ["Bergen County", "NJ", 957736, 645, "med"],
  ["Montgomery County", "MD", 1062061, 615, "med"],
  ["Prince George's County", "MD", 955306, 485, "med"],
  ["Philadelphia County", "PA", 1550542, 255, "high"],
  ["Allegheny County", "PA", 1233253, 235, "med"]
];

const ZIPS = [
  // [zip, name/city, state, pop, price_k]
  ["85001", "Phoenix, AZ", "AZ", 2685, 385],
  ["90001", "Los Angeles, CA", "CA", 61555, 645],
  ["90028", "Los Angeles, CA", "CA", 28158, 825],
  ["92660", "Newport Beach, CA", "CA", 22455, 2250],
  ["94110", "San Francisco, CA", "CA", 74389, 1585],
  ["94301", "Palo Alto, CA", "CA", 17125, 3250],
  ["80202", "Denver, CO", "CO", 6854, 625],
  ["06830", "Greenwich, CT", "CT", 28129, 1950],
  ["33139", "Miami Beach, FL", "FL", 38554, 685],
  ["33301", "Fort Lauderdale, FL", "FL", 8921, 635],
  ["30309", "Atlanta, GA", "GA", 18956, 565],
  ["60614", "Chicago, IL", "IL", 68582, 625],
  ["02116", "Boston, MA", "MA", 20635, 1285],
  ["02139", "Cambridge, MA", "MA", 36932, 1125],
  ["21201", "Baltimore, MD", "MD", 14582, 275],
  ["27601", "Raleigh, NC", "NC", 8125, 485],
  ["28202", "Charlotte, NC", "NC", 12485, 515],
  ["07030", "Hoboken, NJ", "NJ", 55131, 895],
  ["89109", "Las Vegas, NV", "NV", 28852, 425],
  ["10011", "New York, NY", "NY", 48925, 1625],
  ["10014", "New York, NY", "NY", 31865, 1825],
  ["11211", "Brooklyn, NY", "NY", 83125, 1285],
  ["43215", "Columbus, OH", "OH", 12825, 385],
  ["97209", "Portland, OR", "OR", 13485, 585],
  ["19103", "Philadelphia, PA", "PA", 21925, 485],
  ["02903", "Providence, RI", "RI", 18562, 395],
  ["29401", "Charleston, SC", "SC", 3825, 985],
  ["37203", "Nashville, TN", "TN", 9825, 625],
  ["78701", "Austin, TX", "TX", 8625, 785],
  ["75201", "Dallas, TX", "TX", 12485, 525],
  ["77002", "Houston, TX", "TX", 18925, 425],
  ["78205", "San Antonio, TX", "TX", 2125, 325],
  ["84101", "Salt Lake City, UT", "UT", 5825, 485],
  ["22101", "McLean, VA", "VA", 26485, 1285],
  ["98101", "Seattle, WA", "WA", 13582, 785],
  ["98004", "Bellevue, WA", "WA", 36825, 1685]
];

// Window-specific pseudorandom but stable per-geo generator
function mulberry32(seed) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function hash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

const WINDOWS = [
  { key: "30d", days: 30, vol: 1 },
  { key: "90d", days: 90, vol: 3 },
  { key: "180d", days: 180, vol: 6 },
  { key: "1y", days: 365, vol: 12 }
];

function genRows(geo_type, base) {
  const rows = [];
  for (const w of WINDOWS) {
    for (const rec of base) {
      const rand = mulberry32(hash(geo_type + rec.id + w.key));
      const pending_pct = 12 + rand() * 38;            // 12–50%
      const median_dom = 18 + rand() * 55;             // 18–73 days
      const dom_sub60_share = median_dom < 60 ? 0.55 + rand() * 0.45 : 0.1 + rand() * 0.4;
      const priceK = rec.price_k ?? (200 + rand() * 900);
      const volMult = rec.vol_bucket === "high" ? 1.6 : rec.vol_bucket === "low" ? 0.4 : 1;
      const baseVol = geo_type === "state"
        ? 900 * volMult
        : geo_type === "county"
        ? 140 * volMult
        : 3.5 * volMult;
      const homes_sold = Math.max(1, Math.round(baseVol * w.vol * (0.65 + rand() * 0.7)));
      const asOf = new Date();
      rows.push({
        geo_type,
        geo_id: rec.id,
        name: rec.name,
        state: rec.state,
        population: rec.pop ?? null,
        median_sale_price: Math.round(priceK * 1000),
        pending_pct: Math.round(pending_pct * 100) / 100,
        median_dom: Math.round(median_dom * 10) / 10,
        dom_sub60_share: Math.round(dom_sub60_share * 1000) / 1000,
        homes_sold,
        window: w.key,
        as_of: asOf.toISOString().slice(0, 10)
      });
    }
  }
  return rows;
}

const stateRecs = STATES.map(([code, name, pop]) => ({
  id: code, name, state: code, pop, vol_bucket: pop > 10_000_000 ? "high" : pop > 4_000_000 ? "med" : "low"
}));

const countyRecs = COUNTIES.map(([name, state, pop, price_k, vol_bucket]) => ({
  id: `${name}|${state}`, name, state, pop, price_k, vol_bucket
}));

const zipRecs = ZIPS.map(([zip, name, state, pop, price_k]) => ({
  id: zip, name, state, pop, price_k, vol_bucket: "low"
}));

const allRows = [
  ...genRows("state", stateRecs),
  ...genRows("county", countyRecs),
  ...genRows("zip", zipRecs)
];

const dataset = {
  generated_at: new Date().toISOString(),
  source: "seed (will be replaced by Redfin Data Center + Census on first /api/refresh)",
  rows: allRows
};

const outPath = path.join(process.cwd(), "data", "dataset.json");
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(dataset));
console.log(`seed wrote ${allRows.length} rows → ${outPath}`);

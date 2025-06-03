import asyncio
import csv
import os
from playwright.async_api import async_playwright
import re

INPUT_CSV_FILE = "speakers-2.csv"  # Input file with speakers
OUTPUT_CSV_FILE = "duckduckgo_linkedin_profiles.csv"  # Output file to save profile links

async def add_delay(seconds=2):
    """Adds a small delay to avoid detection."""
    await asyncio.sleep(seconds)

def clean_linkedin_url(url):
    """Clean LinkedIn URL to get the standard format."""
    # Extract the main profile part using regex
    match = re.search(r'(https?://[^/]+/in/[^/?#]+)', url)
    if match:
        return match.group(1)
    return url

async def extract_linkedin_from_page(page):
    """Extract LinkedIn profile URL from the current page using multiple strategies."""
    linkedin_profile = None
    
    # Strategy 1: Try to get links directly using JavaScript evaluation
    try:
        links = await page.evaluate("""
            () => {
                // Try multiple selector patterns
                const selectors = [
                    'article a[href*="linkedin.com/in/"]',
                    '.result a[href*="linkedin.com/in/"]',
                    'a[href*="linkedin.com/in/"]'
                ];
                
                for (const selector of selectors) {
                    const elements = document.querySelectorAll(selector);
                    if (elements.length > 0) {
                        return Array.from(elements).map(a => a.href);
                    }
                }
                return [];
            }
        """)
        if links and len(links) > 0:
            linkedin_profile = links[0]  # Only take the first link
            print(f"🎯 Found profile using JavaScript evaluation: {linkedin_profile}")
            return clean_linkedin_url(linkedin_profile)
    except Exception as e:
        print(f"JavaScript evaluation failed: {e}")
    
    # Strategy 2: Try using a more comprehensive selector approach
    try:
        # Get all links on the page
        all_links = await page.query_selector_all("a")
        for link in all_links:
            href = await link.get_attribute('href')
            if href and 'linkedin.com/in/' in href:
                linkedin_profile = href
                print(f"🎯 Found profile using selector approach: {linkedin_profile}")
                return clean_linkedin_url(linkedin_profile)
    except Exception as e:
        print(f"Selector approach failed: {e}")
    
    # Strategy 3: Extract from the HTML content as a last resort
    try:
        # Get all the HTML content
        content = await page.content()
        # Extract LinkedIn URLs using regex
        matches = re.findall(r'https?://(?:www\.)?linkedin\.com/in/[^\s"\'<>]+', content)
        if matches:
            linkedin_profile = matches[0]  # Take only the first match
            print(f"🎯 Found profile using regex extraction: {linkedin_profile}")
            return clean_linkedin_url(linkedin_profile)
    except Exception as e:
        print(f"Regex extraction failed: {e}")
    
    return None

async def scrape_profile_for_speaker(page, speaker, output_csv_path, scraped_profiles):
    """
    Scrapes only the first LinkedIn profile link from DuckDuckGo for a given speaker.
    Tries multiple search strategies if initial search fails.
    Writes results immediately to prevent data loss.
    """
    # Check if name is present
    if not speaker["name"]:
        print(f"⚠️ Skipping entry with missing name")
        return

    # Define multiple search strategies with confidence scores (highest confidence first)
    search_strategies = []
    
    # Strategy 1: Name + Title + Company (Confidence: 4)
    if speaker["title"] and speaker["company"]:
        search_strategies.append({
            "query": f'site:linkedin.com/in "{speaker["name"]}" "{speaker["title"]}" "{speaker["company"]}"',
            "description": "Name + Title + Company",
            "confidence": 4
        })
    
    # Strategy 2: Name + Company (Confidence: 3)
    if speaker["company"]:
        search_strategies.append({
            "query": f'site:linkedin.com/in "{speaker["name"]}" "{speaker["company"]}"',
            "description": "Name + Company",
            "confidence": 3
        })
    
    # Strategy 3: Name + Title (Confidence: 2)
    if speaker["title"]:
        search_strategies.append({
            "query": f'site:linkedin.com/in "{speaker["name"]}" "{speaker["title"]}"',
            "description": "Name + Title",
            "confidence": 2
        })
    
    # Strategy 4: Name only (Confidence: 1)
    search_strategies.append({
        "query": f'site:linkedin.com/in "{speaker["name"]}"',
        "description": "Name only",
        "confidence": 1
    })

    linkedin_profile = None
    strategy_confidence = 0  # Initialize confidence score
    
    # Try each search strategy until we find a result
    for i, strategy in enumerate(search_strategies):
        print(f"\n🔍 Strategy {i+1}/{len(search_strategies)} - {strategy['description']}")
        
        # Truncate query if it's too long (DuckDuckGo has query length limits)
        max_query_length = 200  # Reduced from 400 to avoid truncation
        query = strategy['query']
        if len(query) > max_query_length:
            # Smart truncation - try to keep the most important parts
            if 'company' in strategy['description'].lower():
                # Keep name and company, truncate title if present
                base_query = f'site:linkedin.com/in "{speaker["name"]}" "{speaker["company"]}"'
                if len(base_query) <= max_query_length:
                    query = base_query
                else:
                    query = query[:max_query_length-3] + "..."
            else:
                query = query[:max_query_length-3] + "..."
            print(f"⚠️ Query truncated to: {query}")
        
        print(f"Final query: {query}")
        
        try:
            # Navigate to DuckDuckGo search with URL encoding
            import urllib.parse
            encoded_query = urllib.parse.quote_plus(query)
            search_url = f"https://duckduckgo.com/?q={encoded_query}&ia=web"
            
            await page.goto(search_url, timeout=10000)  # 10 second timeout
            await add_delay(0.5)  # Shorter delay for faster processing
            
            # Wait for results with much shorter timeout - fail fast
            results_found = False
            try:
                await page.wait_for_selector("article[data-testid='result']", timeout=2000)
                results_found = True
            except:
                try:
                    await page.wait_for_selector("article", timeout=2000)
                    results_found = True
                except:
                    try:
                        await page.wait_for_selector(".result", timeout=2000)
                        results_found = True
                    except:
                        print(f"⚠️ No results found for this strategy (timeout after 2s)")
                        continue
            
            if not results_found:
                print(f"⚠️ Results container not found, trying next strategy")
                continue
                
            # Extract LinkedIn profile from current page
            found_profile = await extract_linkedin_from_page(page)
            
            if found_profile:
                linkedin_profile = found_profile
                strategy_confidence = strategy['confidence']  # Store the confidence score
                print(f"✅ Found profile using {strategy['description']} (Confidence: {strategy_confidence}): {linkedin_profile}")
                break
            else:
                print(f"❌ No LinkedIn profile found with {strategy['description']}")
                
        except Exception as e:
            print(f"❌ Error with strategy {i+1}: {e}")
            continue

    # Process the found profile
    if linkedin_profile:
        linkedin_profile = clean_linkedin_url(linkedin_profile)
        
        # Check if this profile has already been scraped
        if linkedin_profile not in scraped_profiles:
            # Immediately write to CSV to prevent data loss
            with open(output_csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["query_name", "query_title", "query_company", "profile_link", "confidence_score"])
                writer.writerow({
                    "query_name": speaker["name"],
                    "query_title": speaker["title"],
                    "query_company": speaker["company"],
                    "profile_link": linkedin_profile,
                    "confidence_score": strategy_confidence
                })
            
            scraped_profiles.add(linkedin_profile)
            print(f"✅ Found profile and saved for {speaker['name']}: {linkedin_profile} (Confidence: {strategy_confidence})")
        else:
            print(f"⚠️ Profile already scraped for {speaker['name']}: {linkedin_profile}")
    else:
        print(f"ℹ️ No LinkedIn profile found for {speaker['name']}")
        
        # Debug: Save HTML if no profile found
        try:
            html = await page.content()
            safe_name = re.sub(r'[^\w\s-]', '', speaker['name']).strip().replace(' ', '_')
            with open(f"debug_{safe_name}.html", "w", encoding="utf-8") as f:
                f.write(html)
            print(f"💾 Saved debug HTML to debug_{safe_name}.html")
        except Exception as e:
            print(f"Could not save debug HTML: {e}")

async def scrape_duckduckgo_for_speakers():
    speakers = []

    # Check if input file exists
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"❌ Input file '{INPUT_CSV_FILE}' not found!")
        return

    # Read the speakers CSV file with exact column names
    with open(INPUT_CSV_FILE, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        
        if not reader.fieldnames:
            print("❌ CSV file appears to be empty or improperly formatted!")
            return

        print(f"📄 CSV Headers Detected: {reader.fieldnames}")
        
        for row in reader:
            # Use exact column names from your CSV file
            speakers.append({
                "name": row.get("Name", "").strip(),
                "title": row.get("Job Title", "").strip(),
                "company": row.get("Company", "").strip()
            })

    print(f"✅ Loaded {len(speakers)} speakers from {INPUT_CSV_FILE}.")
    
    # Filter out speakers with empty names
    speakers = [s for s in speakers if s["name"]]
    print(f"✅ Filtered to {len(speakers)} speakers with valid names.")
    
    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_CSV_FILE)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Create or truncate the output file and write the header
    with open(OUTPUT_CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["query_name", "query_title", "query_company", "profile_link", "confidence_score"])
        writer.writeheader()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # Set to True for production
            args=['--disable-blink-features=AutomationControlled']  # Hide automation
        )
        
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
        )
        
        page = await context.new_page()
        
        # Enable debug logging
        page.on("console", lambda msg: print(f"BROWSER LOG: {msg.text}"))

        scraped_profiles = set()  # Track all scraped profile links
        
        # Iterate over each speaker and search for their LinkedIn profiles
        for i, speaker in enumerate(speakers):
            print(f"\n🚀 Processing speaker {i+1}/{len(speakers)}: {speaker['name']}")
            try:
                await scrape_profile_for_speaker(page, speaker, OUTPUT_CSV_FILE, scraped_profiles)
            except Exception as e:
                print(f"❌ Error processing {speaker['name']}: {e}")
                # Continue with next speaker even if there's an error with the current one
            
            # Add a small delay between searches to avoid rate limiting
            if i < len(speakers) - 1:
                await add_delay(2)  # Reduced delay for faster processing

        print(f"\n✅ Scraping complete. Data saved to {OUTPUT_CSV_FILE}.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_duckduckgo_for_speakers())
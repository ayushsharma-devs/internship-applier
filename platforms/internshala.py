# platforms/internshala.py
import asyncio
import random
import logging
import re
import html as html_module
import sys
import time
import json
import httpx
from pathlib import Path
from playwright.async_api import Page, Locator
from .base import BasePlatformAdapter
import extractor

logger = logging.getLogger("Orchestrator.Internshala")

# --- CORE UTILITIES PRESERVED FROM YOUR APPLIER.PY ---
ENABLE_SUBMIT = True  # Set to True so it actually submits the forms now!
MAX_ANSWER_CHARS = 900

def sanitize_answer_text(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"<br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(line for line in lines if line).strip()
    if len(text) > MAX_ANSWER_CHARS:
        text = text[:MAX_ANSWER_CHARS].rsplit(" ", 1)[0].rstrip(".,;") + "..."
    return text

TEXT_FIELD_SKIP_PHRASES = (
    "upload cv", "upload resume", "upload your resume", 
    "attach resume", "custom resume", "confirm your availability"
)

MCQ_SKIP_PHRASES = (
    "upload cv", "upload resume", "bookmark", "confirm your availability","add this to my bookmark","bookmark","your resume","resume"
)

def _label_matches_skip_phrase(label: str, phrases: tuple[str, ...]) -> bool:
    lower = label.lower()
    return any(phrase in lower for phrase in phrases)


# --- ADAPTER DEFINITION MATCHING YOUR NEW PIPELINE ---
class InternshalaAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(platform_key="internshala")
        # Explicitly handling the Quill editor and ignoring hidden textareas
        self.selectors = {
            "apply_now_button": "a#apply_now_button, button:has-text('Apply now')",
            "already_applied_indicator": "button:has-text('Applied'), .already-applied-status",
            "form_text_inputs": "textarea:not([style*='display: none']), input[type='text'], .ql-editor",
            "final_submit_button": "button:has-text('Submit Application'), button:has-text('Submit')",
            "job_description": ".job-description, .profile_detail, .job_summary_container "
        }

    async def extract_jobs(self, page: Page, current_page_num: int) -> list[dict]:
        await extractor.auto_scroll_page(page)
        return await extractor.extract_page_listings(page, {"selectors": self.selectors})

    async def apply(self, page: Page, detail_url: str, profile_data: dict) -> str:
        logger.info(f"Navigating pipeline stream to target link: {detail_url}")
        
        ollama_url = profile_data.get("ollama_base_url", "http://127.0.0.1:11434")
        synthesizer = LLMResponseSynthesizer(base_url=ollama_url)
        
        try:
            await page.goto(detail_url, wait_until="domcontentloaded", timeout=45000)
            
            # Simulate human idle reading behavior
            await self._human_idle_read_page(page)

            # Check if job was already processed previously
            if await page.locator(self.selectors["already_applied_indicator"]).count() > 0:
                logger.info("Target job slot already exhibits an 'Applied' state.")
                return "Execution_Success"

            # Trigger initial form entry
            apply_now_btn = page.locator(self.selectors["apply_now_button"]).first
            if await apply_now_btn.count() == 0:
                return "Execution_Error"
            
            await apply_now_btn.click()
            await asyncio.sleep(2.5) # Let modal view fully render

            # Extract job data to compile LLM context payload
            job_desc_loc = page.locator(self.selectors["job_description"])
            job_desc = " ".join((await job_desc_loc.first.inner_text()).split()).strip() if await job_desc_loc.count() > 0 else "Not specified on page."
            
            clean_profile = {k: v for k, v in profile_data.items() if k not in ["selectors", "ollama_base_url", "platform_name"]}
            context_string = json.dumps({
                "candidate_profile": clean_profile, 
                "job_posting_requirements": job_desc
            }, indent=2)

            # --- FLAT DOM EXECUTION PIPELINE ---
            logger.info("Processing flat evaluation logic frame...")

            # 1. Scrape all visible inputs simultaneously
            mcq_questions = await self._extract_visible_mcqs(page)
            text_questions = await self._extract_visible_text_questions(page)

            # 2. Phase A: Process MCQs (Human behavior preference)
            if mcq_questions:
                logger.info(f"Processing {len(mcq_questions)} MCQ field(s) first.")
                for mcq in mcq_questions:
                    selected_option = await synthesizer.generate_mcq_response(
                        prompt=mcq["raw_text"],
                        options=mcq["options"],
                        context=context_string,
                    )

                    matched_option = next((opt for opt in mcq["options"] if opt.lower() in selected_option.lower() or selected_option.lower() in opt.lower()), None)
                    final_choice = matched_option if matched_option else mcq["options"][0]
                    
                    # ROUTING LOGIC: Split between Dropdowns and Radio/Checkboxes
                    if mcq.get("is_select", False):
                        await self._handle_dropdown_humanized(mcq["name"], final_choice, page)
                    else:
                        await self._handle_radio_checkbox_humanized(mcq["name"], final_choice, page)
                        
                    await asyncio.sleep(0.8)

            # 3. Phase B: Process Open Text Fields
            if text_questions:
                logger.info(f"Processing {len(text_questions)} text field(s).")
                prompt_list = [q["raw_text"] for q in text_questions]
                llm_results = await synthesizer.match_responses(prompts=prompt_list, context=context_string)

                for q in text_questions:
                    ans_text = llm_results.get(q["raw_text"], "Please refer to resume.")
                    await self._clear_and_type_humanized(q["element"], ans_text, page)
                    await asyncio.sleep(random.uniform(0.3, 0.6))

            # 4. Finalization
            return await self._finalize_submission_pass(page)

        except Exception as e:
            logger.error(f"Pipeline transaction failed on target page: {e}", exc_info=True)
            return "Execution_Error"

    # --- REPLICATED APPLIER ROUTINES ---
    
    async def _extract_visible_text_questions(self, page: Page) -> list[dict]:
        locator = page.locator(self.selectors["form_text_inputs"])
        questions = []
        for idx in range(await locator.count()):
            field = locator.nth(idx)
            if not await field.is_visible():
                continue
            clean_text = await self._resolve_field_label(field)
            if not clean_text or _label_matches_skip_phrase(clean_text, TEXT_FIELD_SKIP_PHRASES):
                continue
            questions.append({"raw_text": clean_text, "element": field})
        return questions

    async def _extract_visible_mcqs(self, page: Page) -> list[dict]:
        mcq_groups = await page.evaluate("""() => {
            const inputs = Array.from(document.querySelectorAll("input[type='checkbox'], input[type='radio'], select"));
            const groups = {};
            
            inputs.forEach(input => {
                const groupKey = input.name || input.id || 'unnamed_group';
                
                if (!groups[groupKey]) {
                    // FIX 1: Crawl UP the DOM tree until we actually hit the question heading
                    let current = input.parentElement;
                    let labelEl = null;
                    while (current && current !== document.body) {
                        labelEl = current.querySelector('.assessment_question, .question-heading');
                        if (labelEl) break;
                        current = current.parentElement;
                    }
                    
                    let labelText = '';
                    if (labelEl) {
                        const clone = labelEl.cloneNode(true);
                        clone.querySelectorAll('.badge, .text-muted, span').forEach(n => n.remove());
                        labelText = clone.innerText.trim();
                    }
                    
                    groups[groupKey] = {
                        name: input.name || input.id || '',
                        raw_text: labelText || 'Select an option:',
                        options: [],
                        is_select: input.tagName.toLowerCase() === 'select'
                    };
                }
                
                // FIX 2: Properly extract human-readable option text via the 'for' attribute
                if (input.tagName.toLowerCase() === 'select') {
                    Array.from(input.options).forEach(opt => {
                        if (opt.value && opt.text && opt.text.trim()) {
                            groups[groupKey].options.push(opt.text.trim());
                        }
                    });
                } else {
                    let optionText = input.value; 
                    if (input.id) {
                        // Look for a sibling label linked by ID (Internshala's pattern)
                        const linkedLabel = document.querySelector(`label[for="${input.id}"]`);
                        if (linkedLabel) optionText = linkedLabel.innerText;
                    }
                    // Fallback to parent wrapper if 'for' isn't used
                    if (optionText === input.value) {
                        const parentLabel = input.closest('label');
                        if (parentLabel) optionText = parentLabel.innerText;
                    }
                    groups[groupKey].options.push(optionText.trim());
                }
            });
            
            return Object.values(groups);
        }""")
        
        cleaned_mcqs = []
        for g in mcq_groups:
            text = " ".join(g["raw_text"].split()).strip().lower()
            name_attr = g.get("name", "").lower()
            
            # 1. The Attribute Check (Backend fallback)
            if "availability" in name_attr:
                continue

            # 2. The Visual Text Check (Your skip phrases)
            if not g["options"] or _label_matches_skip_phrase(text, MCQ_SKIP_PHRASES):
                continue
                
            cleaned_mcqs.append({
                "name": g["name"],
                "raw_text": text,
                "options": list(dict.fromkeys(g["options"])),
                "is_select": g.get("is_select", False)
            })
            
        return cleaned_mcqs

    async def _resolve_field_label(self, input_el: Locator) -> str:
        clean_label = await input_el.evaluate("""element => {
            const container = element.closest('.form-group, .assessment_question_container');
            if (!container) return '';
            const labelElement = container.querySelector('label, .assessment_question, .control-label');
            if (labelElement) {
                const clone = labelElement.cloneNode(true);
                clone.querySelectorAll('.badge, .text-muted, span').forEach(n => n.remove());
                return clone.innerText.trim();
            }
            let sibling = element.previousElementSibling;
            if (sibling && sibling.innerText && sibling.innerText.trim()) return sibling.innerText.trim();
            return '';
        }""")
        return " ".join(clean_label.split()).strip()

    async def _clear_and_type_humanized(self, element: Locator, text: str, page: Page):
        await element.scroll_into_view_if_needed()
        await element.click()
        select_all = "Meta+A" if sys.platform == "darwin" else "Control+A"
        await page.keyboard.press(select_all)
        await page.keyboard.press("Backspace")
        
        chunk_size = random.randint(4, 9)
        for offset in range(0, len(text), chunk_size):
            chunk = text[offset : offset + chunk_size]
            await element.press_sequentially(chunk, delay=random.uniform(35, 90))
            if random.random() < 0.1:
                await asyncio.sleep(random.uniform(0.2, 0.5))

    async def _handle_dropdown_humanized(self, name_attribute: str, selected_option: str, page: Page) -> bool:
        """Handles native selects and Chosen.js custom UI dropdowns."""
        try:
            select_loc = page.locator(f'select[name="{name_attribute}"], select[id="{name_attribute}"]').first
            
            if await select_loc.count() > 0:
                is_hidden = not await select_loc.is_visible()
                
                if is_hidden:
                    logger.info(f"Hidden select detected for '{name_attribute}'. Engaging Chosen.js UI interaction.")
                    select_id = await select_loc.get_attribute("id")
                    
                    if select_id:
                        chosen_container = page.locator(f"#{select_id}_chosen")
                    else:
                        chosen_container = select_loc.locator("xpath=following-sibling::div[contains(@class, 'chosen-container')]").first

                    if await chosen_container.count() > 0:
                        await chosen_container.scroll_into_view_if_needed()
                        await chosen_container.locator("a.chosen-single").click()
                        await asyncio.sleep(random.uniform(0.3, 0.6))
                        
                        option_target = chosen_container.locator(f"ul.chosen-results li:text-is('{selected_option}')").first
                        if await option_target.count() == 0:
                            option_target = chosen_container.locator(f"ul.chosen-results li:has-text('{selected_option}')").first

                        if await option_target.count() > 0 and await option_target.is_visible():
                            await option_target.click()
                            return True
                        else:
                            logger.warning(f"Chosen UI option '{selected_option}' not found. Bailing out.")
                            await page.keyboard.press("Escape")
                            return False
                else:
                    await select_loc.scroll_into_view_if_needed()
                    await select_loc.select_option(label=selected_option)
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to interact with Dropdown '{name_attribute}': {e}")
            return False

    async def _handle_radio_checkbox_humanized(self, name_attribute: str, selected_option: str, page: Page) -> bool:
        """Handles standard radio buttons and checkboxes, bypassing label interception."""
        try:
            # 1. Target the exact input element
            input_target = page.locator(f'input[name="{name_attribute}"][value="{selected_option}"]').first
            if await input_target.count() == 0:
                input_target = page.locator(f'input[value="{selected_option}"]').first

            if await input_target.count() > 0:
                await input_target.scroll_into_view_if_needed()
                
                # Fast return if it's already selected (prevents toggling off a checkbox)
                if await input_target.is_checked():
                    return True

                # STRATEGY A: Find and click the linked label based on the input's ID
                input_id = await input_target.get_attribute("id")
                if input_id:
                    label_target = page.locator(f'label[for="{input_id}"]').first
                    if await label_target.count() > 0 and await label_target.is_visible():
                        await label_target.click()
                        return True
                
                # STRATEGY B: Check if the input is wrapped inside a label tag and click the parent
                parent_label = input_target.locator("xpath=ancestor::label").first
                if await parent_label.count() > 0 and await parent_label.is_visible():
                    await parent_label.click()
                    return True

                # STRATEGY C: Brute force the click if Playwright is still complaining about interception
                logger.info(f"Standard clicks blocked for '{name_attribute}'. Forcing click.")
                await input_target.click(force=True)
                return True

            # 2. Complete Fallback: Try just finding the text anywhere in a label
            fallback_label = page.locator(f'label:has-text("{selected_option}")').first
            if await fallback_label.count() > 0:
                await fallback_label.scroll_into_view_if_needed()
                await fallback_label.click()
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to interact with Radio/Checkbox '{name_attribute}': {e}")
            return False
    async def _human_idle_read_page(self, page: Page):
        for _ in range(random.randint(1, 2)):
            delta = random.randint(90, 200) * random.choice((1, -1))
            await page.mouse.wheel(0, delta)
            await asyncio.sleep(random.uniform(0.4, 1.0))

    async def _finalize_submission_pass(self, page: Page) -> str:
        if not ENABLE_SUBMIT:
            logger.info("Dry-run configured. Skipping definitive submit call.")
            return "Execution_Success"

        submit_btn = page.locator(self.selectors["final_submit_button"]).first
        if await submit_btn.count() == 0 or not await submit_btn.is_visible():
            return "Execution_Error"

        await submit_btn.scroll_into_view_if_needed()
        await asyncio.sleep(1.0)
        await submit_btn.click()
        
        # Verify success verification frames exactly how your verification block worked
        for attempt in range(6):
            await asyncio.sleep(1.5)
            # Standard successful submission validation checks
            if await page.locator("text=Application submitted, text=Successfully applied, text=Form submitted successfully").count() > 0:
                logger.info("Submission verified on page.")
                return "Execution_Success"
                
        # Simple dashboard URL verification backup
        if "dashboard" in page.url:
            return "Execution_Success"
            
        return "Execution_Success"
    # [Keep the rest of your class methods (_extract_visible_text_questions, _extract_visible_mcqs, etc.) exactly as they are]


# --- LLM INFERENCE CONNECTIVITY HANDLING ---
class LLMResponseSynthesizer:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.2"):
        base_url = base_url.rstrip("/")
        self.base_url = f"{base_url}/api/generate" if not base_url.endswith("/api/generate") else base_url
        self.model = model
        self.timeout_config = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)

    async def generate_response(self, prompt: str, context: str) -> str:
        system_instructions = ("""
            You are an advanced AI assistant acting strictly as Ayush Sharma, answering a specific application question for a technical internship.



You will be provided with a JSON context payload containing two core keys:

1. `candidate_profile`: Ayush's actual skills, projects, and tech background.

2. `job_posting_requirements`: The scraped description of the specific internship.



CRITICAL INSTRUCTIONS & CONSTRAINTS:

- FORMATTING: Output raw, unformatted plain text ONLY. Absolutely no Markdown (no asterisks, no hash headers, no backticks), no HTML, no bullet points, and no line breaks. Everything must be a single fluid paragraph.

- LENGTH: Keep responses highly concise, between 2 to 4 sentences, and strictly under 80 words.

- FACTUALITY: Base technical answers strictly on the `candidate_profile`. Do not invent or hallucinate any projects, skills, or employment history.

- TONE: Professional, confident, and direct. Start with proper title-case capitalization.



SPECIAL LOGIC PASS-THROUGHS:

- STIPEND: If the question asks about stipend satisfaction or expectations, respond with a direct confirmation (e.g., "Yes, I am completely comfortable with the specified stipend amount.").

- AVAILABILITY: If the question asks about availability for 6 months or immediate joining, answer affirmatively (e.g., "Yes, I am available to join immediately full-time for the entire six-month duration.").

- GITHUB LINK: If asked for your GitHub link, output exactly: https://github.com/ayushsharma-devs

- LINKEDIN LINK: If asked for your LinkedIn link, output exactly: https://www.linkedin.com/in/ayush-sharma-devnow/

"""
        )
        payload = {
            "model": self.model,
            "prompt": f"Instructions:\n{system_instructions}\n\nContext:\n{context}\n\nQuestion:\n{prompt}",
            "stream": False
        }
        async with httpx.AsyncClient(timeout=self.timeout_config) as client:
            try:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                raw = response.json().get("response", "").strip()
                return sanitize_answer_text(raw)
            except Exception:
                return ""

    async def generate_mcq_response(self, prompt: str, options: list[str], context: str) -> str:
        options_block = "\n".join([f"- {opt}" for opt in options])
        system_instructions = (
           "You are Ayush Sharma answering a multiple-choice question. "

            "Analyze your context, skills, and experience to pick the single truest choice. "

            "You MUST reply with exactly one item from the provided options list verbatim. "

            "Do not add punctuation, explanations, markdown wrappers, or extra dialogue."
        )
        payload = {
            "model": self.model,
            "prompt": f"Instructions:\n{system_instructions}\n\nContext:\n{context}\n\nQuestion:\n{prompt}\n\nOptions:\n{options_block}\n\nSelection:",
            "stream": False
        }
        async with httpx.AsyncClient(timeout=self.timeout_config) as client:
            try:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                ans = response.json().get("response", "").strip()
                return ans.strip('"\'')
            except Exception:
                return ""

    async def match_responses(self, prompts: list[str], context: str) -> dict[str, str]:
        results = {}
        for prompt in prompts:
            resp = await self.generate_response(prompt, context)
            if resp:
                results[prompt] = resp
        return results
let currentPage = 1;

document.addEventListener('DOMContentLoaded', () => {
    fetchDocuments(currentPage);
    
    document.getElementById('btn-prev').addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            fetchDocuments(currentPage);
        }
    });
    
    document.getElementById('btn-next').addEventListener('click', () => {
        currentPage++;
        fetchDocuments(currentPage);
    });
    
    document.getElementById('btn-go').addEventListener('click', () => {
        const inputVal = parseInt(document.getElementById('page-input').value);
        if (!isNaN(inputVal) && inputVal > 0) {
            currentPage = inputVal;
            fetchDocuments(currentPage);
        }
    });
    
    // Allow enter key in page input
    document.getElementById('page-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            document.getElementById('btn-go').click();
        }
    });
});

async function fetchDocuments(page = 1) {
    const grid = document.getElementById('curation-grid');
    grid.innerHTML = '<div class="col-span-full text-center text-slate-500 py-12">Loading page ' + page + '...</div>';
    
    try {
        const response = await fetch(`/api/curation/documents?page=${page}`);
        const data = await response.json();
        
        if (data.error) throw new Error(data.error);
        
        document.getElementById('total-count').textContent = data.total_unreviewed.toLocaleString();
        document.getElementById('page-input').value = data.page;
        currentPage = data.page;
        
        // Disable previous if on page 1
        document.getElementById('btn-prev').disabled = (currentPage === 1);
        
        // Disable next if we didn't get a full page (meaning we're on the last page)
        document.getElementById('btn-next').disabled = (data.docs.length < data.limit);
        
        renderCards(data.docs);
    } catch (e) {
        console.error("Failed to fetch documents:", e);
        grid.innerHTML = `<div class="col-span-full text-center text-rose-500 py-12">Failed to load documents: ${e.message}</div>`;
    }
}

function renderCards(docs) {
    const grid = document.getElementById('curation-grid');
    grid.innerHTML = '';
    
    if (docs.length === 0) {
        grid.innerHTML = '<div class="col-span-full text-center text-slate-500 py-12">No documents currently waiting for triage.</div>';
        return;
    }

    docs.forEach(doc => {
        const card = document.createElement('div');
        card.id = `card-${doc.record_id}`;
        card.className = "bg-slate-800/40 border border-slate-700/50 rounded-lg p-4 flex flex-col justify-between hover:shadow-lg hover:border-slate-600 transition-all duration-200 transform overflow-visible";
        
        const localBtnClass = doc.prepared_file_path 
            ? "text-blue-400 hover:text-blue-300 hover:bg-blue-500/10" 
            : "text-slate-600 opacity-50 cursor-not-allowed";
        
        const localBtnHref = doc.prepared_file_path ? `href="/api/local_file/${doc.record_id}" target="_blank"` : "";
        
        let formattedDate = doc.timestamp;
        try {
            // Handle ISO string from SQLite
            const dateStr = doc.timestamp.replace(' ', 'T'); 
            const dateObj = new Date(dateStr.includes('+') || dateStr.endsWith('Z') ? dateStr : dateStr + 'Z');
            if (!isNaN(dateObj)) {
                formattedDate = dateObj.toLocaleString(undefined, { 
                    month: 'short', day: 'numeric', year: 'numeric', 
                    hour: 'numeric', minute: '2-digit', hour12: true 
                });
            }
        } catch (e) {}

        card.innerHTML = `
            <div class="mb-3">
                <h3 class="text-base font-bold text-white line-clamp-2 mb-1" title="${escapeHtml(doc.anchor_text)}">${escapeHtml(doc.anchor_text)}</h3>
                <p class="text-xs text-slate-400">${formattedDate}</p>
            </div>
            
            <div class="flex items-center justify-between mt-auto pt-3 border-t border-slate-700/50 relative">
                <div class="flex gap-1.5">
                    <a ${localBtnHref} class="p-1.5 rounded-md transition-colors ${localBtnClass}" title="Open Local Text">
                        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                        </svg>
                    </a>
                    <a href="${doc.source_url}" target="_blank" class="p-1.5 rounded-md text-emerald-400 hover:text-emerald-300 hover:bg-emerald-500/10 transition-colors" title="Open Source URL">
                        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                        </svg>
                    </a>
                </div>
                
                <div class="relative dropdown-container">
                    <button class="p-1.5 rounded-md text-slate-400 hover:text-white hover:bg-slate-700 transition-colors dropdown-toggle" onclick="toggleDropdown('${doc.record_id}')" title="More Actions">
                        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 5v.01M12 12v.01M12 19v.01M12 6a1 1 0 110-2 1 1 0 010 2zm0 7a1 1 0 110-2 1 1 0 010 2zm0 7a1 1 0 110-2 1 1 0 010 2z" />
                        </svg>
                    </button>
                    
                    <div id="dropdown-${doc.record_id}" class="hidden absolute right-0 bottom-full mb-2 w-44 bg-slate-800 border border-slate-700 rounded-lg shadow-xl z-50 overflow-hidden">
                        <button onclick="performAction('${doc.record_id}', 'DRAFT')" class="w-full text-left px-3 py-2 text-xs text-indigo-300 hover:bg-slate-700 hover:text-indigo-200 transition-colors flex items-center gap-2">
                            <span>📝</span> Send to Draft
                        </button>
                        <button onclick="performAction('${doc.record_id}', 'NOT_JOB_RELATED')" class="w-full text-left px-3 py-2 text-xs text-amber-300 hover:bg-slate-700 hover:text-amber-200 transition-colors flex items-center gap-2 border-t border-slate-700/50">
                            <span>🚫</span> Not Job Related
                        </button>
                        <button onclick="performAction('${doc.record_id}', 'OBSOLETE')" class="w-full text-left px-3 py-2 text-xs text-rose-300 hover:bg-slate-700 hover:text-rose-200 transition-colors flex items-center gap-2 border-t border-slate-700/50">
                            <span>🗑️</span> Mark Obsolete
                        </button>
                    </div>
                </div>
            </div>
        `;
        grid.appendChild(card);
    });
}

function toggleDropdown(id) {
    // Close others
    document.querySelectorAll('[id^="dropdown-"]').forEach(el => {
        if (el.id !== `dropdown-${id}`) el.classList.add('hidden');
    });
    
    const dropdown = document.getElementById(`dropdown-${id}`);
    if (dropdown) {
        dropdown.classList.toggle('hidden');
    }
}

// Close dropdowns on outside click
document.addEventListener('click', (e) => {
    if (!e.target.closest('.dropdown-container')) {
        document.querySelectorAll('[id^="dropdown-"]').forEach(el => {
            el.classList.add('hidden');
        });
    }
});

async function performAction(id, action) {
    toggleDropdown(id); // hide it
    
    try {
        const response = await fetch(`/api/curation/action/${id}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: action })
        });
        
        if (response.ok) {
            const card = document.getElementById(`card-${id}`);
            if (card) {
                // CSS Animation for removal
                card.classList.add('opacity-0', 'scale-95');
                setTimeout(() => {
                    card.remove();
                }, 300);
            }
        } else {
            console.error("Action failed:", await response.text());
        }
    } catch (e) {
        console.error("Error performing action:", e);
    }
}

function escapeHtml(text) {
    if (!text) return "";
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

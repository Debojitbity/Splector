/**
 * Splector Database Viewer — DataTables Integration
 *
 * Server-side processing with table switching.
 */

// =========================================================
// TABLE CONFIG
// =========================================================

const TABLE_COLUMNS = {
    stage4_final_docs: [
        { title: 'ID', width: '60px' },
        { title: 'Parent Index Page' },
        { title: 'Final Target URL' },
        { title: 'Anchor Text' },
        { title: 'Extracted At', width: '150px' },
    ],
    stage3_filtered: [
        { title: 'ID', width: '60px' },
        { title: 'Base Domain' },
        { title: 'Filtered URL' },
        { title: 'Anchor Text' },
        { title: 'Filtered At', width: '150px' },
    ],
    stage2_discovered: [
        { title: 'ID', width: '60px' },
        { title: 'Base Domain' },
        { title: 'Discovered URL' },
        { title: 'Anchor Text' },
        { title: 'Discovered At', width: '150px' },
    ],
};


// =========================================================
// INITIALIZE DATATABLE
// =========================================================

let dataTable = null;
let currentTable = 'stage4_final_docs';

$(document).ready(function () {
    initDataTable(currentTable);
});

function initDataTable(tableName) {
    // Destroy existing table if any
    if (dataTable) {
        dataTable.destroy();
        $('#data-table').empty();
    }

    currentTable = tableName;
    const columns = TABLE_COLUMNS[tableName] || TABLE_COLUMNS.stage4_final_docs;

    // Update table headers
    const thead = $('<thead><tr></tr></thead>');
    columns.forEach(col => {
        thead.find('tr').append(`<th>${col.title}</th>`);
    });
    $('#data-table').append(thead).append('<tbody></tbody>');

    dataTable = $('#data-table').DataTable({
        processing: true,
        serverSide: true,
        ajax: {
            url: '/api/data',
            data: function (d) {
                d.table = currentTable;
            },
        },
        columns: columns.map((col, i) => ({
            data: i,
            width: col.width || undefined,
            render: function (data) {
                if (data === null || data === undefined) return '—';
                // Make URLs clickable
                if (typeof data === 'string' && (data.startsWith('http://') || data.startsWith('https://'))) {
                    const truncated = data.length > 80 ? data.substring(0, 80) + '…' : data;
                    return `<a href="${data}" target="_blank" rel="noopener" class="text-blue-400 hover:text-blue-300 hover:underline transition-colors">${truncated}</a>`;
                }
                return data;
            },
        })),
        pageLength: 25,
        lengthMenu: [10, 25, 50, 100],
        order: [[0, 'desc']],
        language: {
            search: '',
            searchPlaceholder: 'Search records...',
            emptyTable: 'No data available. Run the pipeline to populate this table.',
            zeroRecords: 'No matching records found.',
            info: 'Showing _START_ to _END_ of _TOTAL_ entries',
            infoEmpty: 'No entries to show',
            infoFiltered: '(filtered from _MAX_ total)',
            processing: '<div class="flex items-center gap-2 text-slate-400"><svg class="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg> Loading...</div>',
        },
        drawCallback: function (settings) {
            // Update total records badge
            const info = this.api().page.info();
            document.getElementById('total-records').textContent =
                info.recordsTotal.toLocaleString();
        },
        dom: '<"flex flex-wrap items-center justify-between gap-4 mb-4"lf>rt<"flex flex-wrap items-center justify-between gap-4 mt-4"ip>',
    });
}


// =========================================================
// TABLE SWITCHING
// =========================================================

function switchTable() {
    const selector = document.getElementById('table-selector');
    if (selector) {
        initDataTable(selector.value);
    }
}

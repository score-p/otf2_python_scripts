function update_resource_summary(data) {
    table = document.getElementById("summary_table");
    keys = Object.keys(data.summary).sort();
    $('#summary_table tr').each(function(){
        key = keys.shift();
        $(this).find('th').each(function(i, c){
            c.textContent = key;
        });
        $(this).find('td').each(function(i, c){
            c.textContent = data.summary[key];
        });
    })
}

function resource_summary_select_changed(event) {
    event.preventDefault();
    selector = document.getElementById("summary_selector");
    selected_space = selector.options[selector.selectedIndex].value;
    $.getJSON($SCRIPT_ROOT + '/_get_resource_summary', {
        selected_res: selected_space,
    }, update_resource_summary );
}

function init_resource_chart(event) {
    selector = document.getElementById("resource_chart_select");
    selected_thread = selector.options[selector.selectedIndex].value;
    $.getJSON($SCRIPT_ROOT + '/_get_space_util_for_thread', {
        selected_thread: selected_thread,
    }, function(data) {
        space_chart_ctx = document.getElementById("space_chart").getContext("2d");
        space_data = {
                labels: data.labels,
                datasets: [{
                                data: data.data,
                                backgroundColor: data.colors,
                        }],
        };
        window.space_util_chart = new Chart(space_chart_ctx, {
            type: 'doughnut',
            data: space_data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
            },
        });
    });
}

function update_resource_chart(data) {
    space_data = {
                labels: data.labels,
                datasets: [{
                                data: data.data,
                                backgroundColor: data.colors,
                        }],
    };
    window.space_util_chart.data = space_data;
    window.space_util_chart.update();
}

function resource_chart_select_changed(ev) {
    ev.preventDefault();
    selector = document.getElementById("resource_chart_select");
    selected_thread = selector.options[selector.selectedIndex].value;
    $.getJSON($SCRIPT_ROOT + '/_get_space_util_for_thread', {
        selected_thread: selected_thread,
    }, update_resource_chart );
}

function update_thread_chart(data) {
    window.dist_chart.data.labels = data.labels;
    window.dist_chart.data.datasets[0].data = data.stores;
    window.dist_chart.data.datasets[1].data = data.loads;
    window.dist_chart.update();
}

function thread_chart_changed(ev) {
    ev.preventDefault();
    selector = document.getElementById("thread_chart_select");
    selected_space = selector.options[selector.selectedIndex].value;
    $.getJSON($SCRIPT_ROOT + '/_get_thread_stats_per_space', {
        selected_space: selected_space,
    }, update_thread_chart );
}


function init_thread_chart(event) {
    selector = document.getElementById("thread_chart_select");
    selected_space = selector.options[selector.selectedIndex].value;

    $.getJSON($SCRIPT_ROOT + '/_get_thread_stats_per_space', {
        selected_space: selected_space,
    }, function(data) {
        chart_data = {
            labels: data.labels,
            datasets: [{
                label: 'Stores',
                backgroundColor: '#ff6384',
                data: data.stores
            }, {
                label: 'Loads',
                backgroundColor: '#36a2eb',
                data: data.loads
            }
            ]
        };

        ctx = document.getElementById("thread_chart").getContext("2d");

        window.dist_chart = new Chart(ctx, {
            type: 'horizontalBar',
            data: chart_data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    xAxes: [{
                        stacked: true
                    }],
                    yAxes: [{
                        stacked: true
                    }]
                }
            }
        });
    });
}

function init_chart_listener() {
    selector = document.getElementById("resource_chart_select");
    selector.addEventListener("change", resource_chart_select_changed);

    thread_chart_select = document.getElementById("thread_chart_select");
    thread_chart_select.addEventListener("change", thread_chart_changed);

    document.addEventListener("DOMContentLoaded", init_thread_chart);
    document.addEventListener("DOMContentLoaded", init_resource_chart);

    summary_selector = document.getElementById("summary_selector");
    summary_selector.addEventListener("change", resource_summary_select_changed);
}

init_chart_listener();
'use strict';

var defaultFormat = d3.time.format('%b %d %H:00');
function formatLabel(d) {
    return defaultFormat(d);
}

$(document).ready(function() {
    //json data that we intend to update later on via on-screen controls
    var split_by_data;

    var torso = {};
    torso.width = 375;
    torso.height = 200;
    torso.right = 20;

    var trunk = {};
    trunk.width = 320;
    trunk.height = 150;
    trunk.left = 35;
    trunk.right = 10;
    trunk.xax_count = 5;

    var small = {};
    small.width = 240;
    small.height = 140;
    small.left = 20;
    small.right = 20;
    small.top = 20;
    small.xax_count = 5;

    assignEventListeners();

    d3.json('data/errors.json', function(data_in) {
        var allData = {};
        var serieses = ["Bad_Record_Percentage", "Bad_Records",
            "UUID_Bad_Record_Percentage", "Interesting_Bad_Record_Percentage",
            "Records_Read", "Records_Read_Per_Second", "bad_payload",
            "conversion_error", "corrupted_data", "empty_data", "invalid_path",
            "missing_revision", "missing_revision_repo", "uuid_only_path",
            "write_failed"];
        serieses.forEach(function(series){
            allData[series] = [];
        });
        var hours = Object.keys(data_in);
        hours.sort();
        //"2014-06-18T19:00:00"
        var fff = d3.time.format.iso;
        hours.forEach(function(hour){
            var hourDate = fff.parse(hour);
            serieses.forEach(function(series){
                var v = data_in[hour][series];
                if (!v) v = 0;
                allData[series].push({ date: hourDate, value: v })
            });
        });

        moz_chart({
            title: "Processing Rate (Records/sec)",
            description: "Processing rate: Number of records processed per second.",
            data: allData["Records_Read_Per_Second"],
            width: torso.width,
            height: torso.height,
            right: torso.right,
            target: '#records_per_sec',
            x_accessor: 'date',
            y_accessor: 'value',
            //xax_format: formatLabel
            rollover_callback: function(d, i) {
                $('div#records_per_sec svg .active_datapoint').html(formatLabel(d.date) + ': ' + d.value);
            },
        });

        var err_baselines = [{value:10, label:'overall error threshold'}, {value: 1, label: 'interesting error threshold'}]
        split_by_data = moz_chart({
            title: "Errors (%)",
            description: "Rate of overall errors and interesting errors.",
            data: [allData["Bad_Record_Percentage"], allData["Interesting_Bad_Record_Percentage"]],
            width: trunk.width * 2,
            height: trunk.height * 2,
            right: trunk.right,
            baselines: err_baselines,
            target: '#error_rate',
            x_accessor: 'date',
            y_accessor: 'value',
            //xax_format: formatLabel
            rollover_callback: function(d, i) {
                $('div#error_rate svg .active_datapoint').html(
                    formatLabel(d.date) + ': ' + d3.round(d.value, 3));
            },
        });

        moz_chart({
            title: "UUID-only record rate (%)",
            description: "Rate of submissions from old Firefox versions. This is usually what triggers error alerts.",
            data: [allData["Bad_Record_Percentage"], allData["UUID_Bad_Record_Percentage"]],
            width: torso.width,
            height: torso.height,
            right: torso.right,
            baselines: [err_baselines[0]],
            target: '#uuid_only_rate',
            x_accessor: 'date',
            y_accessor: 'value',
            //xax_format: formatLabel
            rollover_callback: function(d, i) {
                $('div#uuid_only_rate svg .active_datapoint').html(
                    formatLabel(d.date) + ': ' + d3.round(d.value, 3));
            },
        });
    });

    function assignEventListeners() {
        $('.modify-time-period-controls button').click(function() {
            var past_n_days = $(this).data('time_period');
            var data = modify_time_period(split_by_data, past_n_days);

            //change button state
            $(this).addClass('active')
                .siblings()
                .removeClass('active');

            //update data
            moz_chart({
                data: data,
                width: trunk.width * 2,
                height: trunk.height * 2,
                right: trunk.right,
                show_years: false,
                transition_on_update: false,
                xax_count: 4,
                target: 'div#error_rate',
                x_accessor: 'date',
                y_accessor: 'value'
            })
        })
    }

})

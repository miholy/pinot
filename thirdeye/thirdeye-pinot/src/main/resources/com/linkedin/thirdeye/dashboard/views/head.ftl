<head>
    <title>ThirdEye Dashboard</title>
    <link rel="stylesheet" href="../../../assets/css/reset.css"/>
    <!--<link rel="stylesheet" href="../../../assets/css/uikit/uikit.css"/>-->
    <link rel="stylesheet" href="../../../assets/css/uikit/uikit.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/uikit.docs.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/docs.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/components/sticky.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/components/sortable.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/components/autocomplete.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/components/form-select.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/uikit/components/datepicker.almost-flat.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/datatables.min.css"/>
    <link rel="stylesheet" href="../../../assets/css/main.css"/>
    <link rel="stylesheet" href="../../../assets/css/d3.css"/>
    <link rel="stylesheet" href="../../../assets/css/c3.css"/>
    <link rel="shortcut icon" href="/assets/img/chakra-s.png">


    <!-- JQuery Google API -->
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.1/jquery.min.js"></script>

    <!-- Fallback in case Google API was not available for the users browser -->
    <script>
        if (typeof jQuery === 'undefined') {
            document.write(unescape('%3Cscript%20src%3D%22lib/jquery-1.10.2.min.js%22%3E%3C/script%3E'));
        }
    </script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/handlebars.js/4.0.5/handlebars.min.js"></script>
	<script src="https://d3js.org/d3.v3.min.js" charset="utf-8"  defer></script>
	<script src="../../../assets/js/d3/nvtooltip.js"  defer></script>
	<script src="../../../assets/js/d3/d3legend.js"  defer></script>
	<script src="../../../assets/js/d3/d3line.js"  defer></script>
	<script src="../../../assets/js/d3/d3linewithlegend.js"  defer></script>
   <script src="../../../assets/js/c3/c3.js"  defer></script>

    <!--Compiled JSTZ.min.js, Moment.min.js, DataTables 1.10.12, jquery.dataTables.columnFilter.min.js, moment-timezone-with-data-2010-2020.min.js into vendorplugins.compiled.js-->
   <script src="../../../assets/js/vendor/vendorplugins.compiled.js" defer></script>
   <script src="../../../assets/js/uikit/uikit.min.js" defer></script>
   <script src="../../../assets/js/uikit/core/dropdown.min.js" defer></script>
   <!--Compiled uikit components as used-components.compiled.js consists of: sticky.min.js, datepicker.min.js, timepicker.min.js,
   autocomplete.min.js, sortable.min.js, form-select.min.js-->
    <script src="../../../assets/js/uikit/components/used-components.compiled.js" defer></script>
    <script src="../../../assets/js/lib/utility.js" defer></script>
    <script src="../../../assets/js/lib/get-form-data.js" defer></script>
    <script src="../../../assets/js/lib/tabular.js" defer></script>
    <script src="../../../assets/js/lib/contributors.js" defer></script>
    <script src="../../../assets/js/lib/heatmap.js" defer></script>
    <script src="../../../assets/js/lib/timeseries.js" defer></script>
    <script src="../../../assets/js/lib/anomalies.js" defer></script>
    <script src="../../../assets/js/lib/self-service.js" defer></script>
    <script src="../../../assets/js/lib/custom-dashboard.js" defer></script>
    <script src="../../../assets/js/lib/handlebars-methods.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-header.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-form.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-form-filter.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-form-time.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-form-submit.js" defer></script>
    <script src="../../../assets/js/lib/dashboard-chart-area.js" defer></script>
    <script src="../../../assets/js/dashboard.js" defer></script>
</head>

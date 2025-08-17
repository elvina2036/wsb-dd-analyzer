/** ===================== utils_debug_menu ===================== **/

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('WSB')
    .addItem('Refresh Symbols (US listings)', '')
    .addItem('Fetch WSB DD posts', '')
    .addSeparator()
    .addItem('1) Sync FE Fixed Data (posts → fe_fixed)', '')
    .addItem('2) Determine Direction (symbols → fe_fixed)', '')
    .addItem('3) Mapping Ticker (symbols → fe_fixed)', '')
    .addItem('4) Refresh ticker cache', '')
    .addItem('5) Refresh ticker cache', '')
    .addItem('6) Sync FE Datas (ticker_cache → both fe_fixed & fe_live)', '')
    .addSeparator()
    .addItem('Run ALL (1 → 6)', '')
    .addSeparator()
    .addItem('Setup schedule (Delete All Triggers & Create New Triggers)','')
    .addToUi();
}

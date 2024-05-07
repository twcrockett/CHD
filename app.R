library(shiny)
library(DBI)
library(RPostgres)
library(reactable)
library(shinythemes)
library(highcharter)

# Database connection
conn_args <- config::get("dataconnection")

con <- dbConnect(Postgres(), 
                 dbname = conn_args$DATABASE_NAME,
                 host = conn_args$DATABASE_HOST,
                 port = conn_args$DATABASE_PORT,
                 user = conn_args$DATABASE_USER,
                 password = conn_args$DATABASE_PASS
)

# Define UI
ui <- fluidPage(
  theme = shinytheme("lumen"),
  titlePanel(windowTitle = "CHD | Shiny",
    div("Congressional Hearing Database",
        div("Taylor Crockett: EPPS 6354.001", class="text-muted", style="font-size: medium; margins: 0 10px;"))),
  sidebarLayout(
    sidebarPanel(
      textInput("speaker", "Speaker Name"),
      textInput("keyword", "Keyword"),
      selectInput("congress", "Congress Session", choices = c("", 111:118)),
      dateRangeInput("dateRange", "Date Range", start = "2000-01-01"),
      highchartOutput("keywordPieChart")
    ),
    mainPanel(
      reactableOutput("results")
    )
  )
)

# Define server logic
server <- function(input, output, session) {
  data <- reactive({
    startDate <- ifelse(is.null(input$dateRange[1]), '1900-01-01', as.character(input$dateRange[1]))
    endDate <- ifelse(is.null(input$dateRange[2]), '2100-01-01', as.character(input$dateRange[2]))
    
    query <- sprintf('
      SELECT m.img_url, d."bioguideId", d."packageId", d.speaker, d.dialogue, h.congress, h."dateIssued", h.title
      FROM dialogue d
      LEFT JOIN member m ON d."bioguideId" = m."bioguideId"
      LEFT JOIN hearing h ON d."packageId" = h."packageId"
      WHERE d.speaker LIKE \'%%%s%%\'
      AND d.dialogue LIKE \'%%%s%%\' 
      AND CAST(h.congress AS TEXT) LIKE \'%%%s%%\'
      AND CAST(h."dateIssued" AS DATE) BETWEEN \'%s\' AND \'%s\';
      ', input$speaker, input$keyword, as.character(input$congress), startDate, endDate
    )
    dbGetQuery(con, query)
  })
  
  totalrow <- reactive({
    startDate <- ifelse(is.null(input$dateRange[1]), '1900-01-01', as.character(input$dateRange[1]))
    endDate <- ifelse(is.null(input$dateRange[2]), '2100-01-01', as.character(input$dateRange[2]))
    query <- sprintf('
      SELECT m.img_url, d."bioguideId", d."packageId", d.speaker, d.dialogue, h.congress, h."dateIssued", h.title
      FROM dialogue d
      LEFT JOIN member m ON d."bioguideId" = m."bioguideId"
      LEFT JOIN hearing h ON d."packageId" = h."packageId"
      WHERE d.speaker LIKE \'%%%s%%\'
      AND CAST(h.congress AS TEXT) LIKE \'%%%s%%\'
      AND CAST(h."dateIssued" AS DATE) BETWEEN \'%s\' AND \'%s\';
      ', input$speaker, as.character(input$congress), startDate, endDate
    )
    dbGetQuery(con, query) %>% nrow()
  })
  
  output$results <- renderReactable({
    reactable(data(), 
      columns = list(
        img_url = colDef(
          name = "",
          html = TRUE,
          cell = function(value, index) {
            bioguideId = data()[index, "bioguideId"]
            sprintf('<a href="https://bioguide.congress.gov/search/bio/%s"><img src="%s" style="height: 50px; width: auto;"></a>', bioguideId, value)
          },
          width = 75 
        ),
        bioguideId = colDef(
          show = FALSE
        ),
        packageId = colDef(
          show = FALSE
        ),
        speaker = colDef(
          name = "Speaker",
          width = 150  
        ),
        dialogue = colDef(
          name = "Dialogue",
          width = 500  
        ),
        congress = colDef(
          name = "Congress",
          width = 100
        ),
        dateIssued = colDef(
          name = "Date Issued",
          width = 100
        ),
        title = colDef(
          name = "Hearing Title",
          html = TRUE,
          width = 200,
          cell = function(value, index) {
            packageId = data()[index, "packageId"]
            url = sprintf("https://www.govinfo.gov/content/pkg/%s/html/%s.htm", packageId, packageId)
            sprintf('<a href="%s">%s</a>', url, value)
          }
        )
      ),
      pagination = TRUE
    )
  })
  
  output$keywordPieChart <- renderHighchart({
    if(is.null(input$keyword) || input$keyword == "") return()
    
    highchart() %>%
      hc_add_series(type = "pie", name = "Dialogues", data = list(
        list(name = "Contains keyword", y = nrow(data()), color = "#4287f5"),
        list(name = "Else", y = totalrow() - nrow(data()), color = "#e1e6ed")
      )) %>%
      hc_plotOptions(pie = list(
        allowPointSelect = TRUE,
        cursor = "pointer",
        dataLabels = list(enabled = TRUE),
        showInLegend = TRUE
      ))
  })
}

# Run the application
shinyApp(ui = ui, server = server)

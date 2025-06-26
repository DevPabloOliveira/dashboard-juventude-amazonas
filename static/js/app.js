let ageChart, raceChart, map, geojsonLayer, legendControl;
let currentMapMetric = 'vulnerabilidade';
let currentAgeGroup = 'geral';
let currentLocation = 'geral';
let isMapReady = false;

// Configuração das Métricas para o Mapa e Legendas
const metricsConfig = {
    vulnerabilidade: {
        property: 'vul_score_final', label: 'Score de Vulnerabilidade', legendTitle: 'Score de Vulnerabilidade',
        grades: [0.48, 0.50, 0.52, 0.55], colors: ['#a5d6a7', '#fff59d', '#ffc107', '#ffa000', '#d32f2f'],
        format: (val) => val !== null ? val.toFixed(3) : 'N/A'
    },
    renda: {
        property: 'renda_media_final', label: 'Renda Média', legendTitle: 'Renda Média (R$)',
        grades: [800, 1000, 1200, 1500], colors: ['#fee5d9', '#fcae91', '#fb6a4a', '#de2d26', '#a50f15'].reverse(),
        format: (val) => val !== null ? `R$ ${val.toLocaleString('pt-BR', {maximumFractionDigits: 0})}` : 'N/A'
    },
    alfabetizacao: {
        property: 'taxa_alfabetizacao_jovens', label: 'Taxa de Alfabetização', legendTitle: 'Taxa de Alfabetização (%)',
        grades: [80, 85, 90, 95], colors: ['#fee5d9', '#fcae91', '#fb6a4a', '#de2d26', '#a50f15'].reverse(),
        format: (val) => val !== null ? `${val.toFixed(1)}%` : 'N/A'
    },
    populacao: {
        property: 'total_jovens', label: 'População Jovem', legendTitle: 'Nº de Jovens',
        grades: [1000, 5000, 10000, 50000], colors: ['#eff3ff', '#bdd7e7', '#6baed6', '#3182bd', '#08519c'],
        format: (val) => val !== null ? val.toLocaleString('pt-BR') : 'N/A'
    }
};
const CHART_COLORS = ['#00796b', '#ffc107', '#1976d2', '#d32f2f', '#5e35b1', '#fdd835'];

document.addEventListener('DOMContentLoaded', initDashboard);

function initDashboard() {
    populateMunicipioFilter();
    initializeMap();
    addEventListeners();
    updateAllData();

    const hoje = new Date();
    if (hoje.getMonth() === 5) { 
        const seasonalCard = document.getElementById('seasonal-highlight-section');
        if (seasonalCard) seasonalCard.style.display = 'block';
    }
}

function addEventListeners() {
    document.getElementById('municipio-select').addEventListener('change', (e) => {
        currentLocation = e.target.value;
        updateAllData();
    });
    document.getElementById('age-select').addEventListener('change', (e) => {
        currentAgeGroup = e.target.value;
        updateAllData();
    });
    document.getElementById('map-metric-select').addEventListener('change', (e) => {
        currentMapMetric = e.target.value;
        updateMapAndRanking();
    });
}

function updateAllData() {
    updateDashboardData();
    updateMapAndRanking();
}

async function updateDashboardData() {
    const url = currentLocation === 'geral' 
        ? `/api/geral?age_group=${currentAgeGroup}`
        : `/api/municipio/${currentLocation}?age_group=${currentAgeGroup}`;
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        updateKpiCards(data);
        updateCharts(data);
    } catch (error) {
        console.error("Falha ao buscar dados do dashboard:", error);
    }
}

async function updateMapAndRanking() {
    try {
        const mapResponse = await fetch(`/api/mapa?age_group=${currentAgeGroup}`);
        const mapData = await mapResponse.json();
        if (geojsonLayer) {
            geojsonLayer.clearLayers().addData(mapData);
        }
        updateLegend();
        await updateRankingCard();

        if (!isMapReady) {
            setTimeout(() => {
                map.invalidateSize(true);
                isMapReady = true;
            }, 100);
        }

    } catch (error) {
        console.error("Falha ao atualizar mapa e ranking:", error);
    }
}

function updateKpiCards(data) {
    const ageTextMap = {
        'geral': '(15-29 anos)', '15-19': '(15-19 anos)',
        '20-24': '(20-24 anos)', '25-29': '(25-29 anos)'
    };
    const ageLabel = ageTextMap[currentAgeGroup];

    // Atualiza Card 1: Total de Jovens
    document.getElementById('kpi-total-jovens-title').textContent = `Total de Jovens ${ageLabel}`;
    document.getElementById('kpi-total-jovens').querySelector('.kpi-number').textContent = data.total_jovens.toLocaleString('pt-BR');

    // Atualiza Card 2: Renda Média
    const locationLabel = currentLocation === 'geral' ? '(Estado)' : '(Município)';
    document.getElementById('kpi-renda-media-title').textContent = `Renda Média ${locationLabel}`;
    document.getElementById('kpi-renda-media').querySelector('.kpi-number').textContent = data.renda_media.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    // Atualiza Card 3: Taxa de Alfabetização
    document.getElementById('kpi-taxa-alfabetizacao-title').textContent = `Taxa de Alfabetização ${ageLabel}`;
    document.getElementById('kpi-taxa-alfabetizacao').querySelector('.kpi-number').textContent = data.taxa_alfabetizacao_jovens.toFixed(1);
}

function updateCharts(data) {
    const ageData = {
        labels: Object.keys(data.distribuicao_etaria),
        values: Object.values(data.distribuicao_etaria)
    };

    // ===== CORREÇÃO APLICADA =====
    const raceData = {
        labels: ['Parda', 'Branca', 'Indígena', 'Preta', 'Amarela'],
        values: [0, 0, 0, 0, 0] 
    };

    // Preenche os valores na ordem correta, garantindo que a propriedade exista
    if (data && data.distribuicao_raca) {
        raceData.values = [
            data.distribuicao_raca.parda,
            data.distribuicao_raca.branca,
            data.distribuicao_raca.indigena,
            data.distribuicao_raca.preta,
            data.distribuicao_raca.amarela
        ];
    }
    // ===================================

    // Atualiza o gráfico de Faixa Etária
    if (ageChart) ageChart.destroy();
    ageChart = new Chart(document.getElementById('ageDistributionChart'), {
        type: 'bar',
        data: { labels: ageData.labels, datasets: [{ label: 'População', data: ageData.values, backgroundColor: CHART_COLORS }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });

    // Atualiza o gráfico de Raça/Cor
    if (raceChart) raceChart.destroy();
    raceChart = new Chart(document.getElementById('raceDistributionChart'), {
        type: 'doughnut',
        data: { labels: raceData.labels, datasets: [{ data: raceData.values, backgroundColor: CHART_COLORS, borderWidth: 2, borderColor: '#fff' }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
    });
}

async function populateMunicipioFilter() {
    try {
        const response = await fetch('/api/municipios');
        const municipios = await response.json();
        const select = document.getElementById('municipio-select');
        select.innerHTML = '<option value="geral">Amazonas (Estado)</option>';
        municipios.forEach(m => {
            const option = document.createElement('option');
            option.value = m;
            option.textContent = m.charAt(0).toUpperCase() + m.slice(1).toLowerCase();
            select.appendChild(option);
        });
    } catch (error) {
        console.error("Falha ao carregar lista de municípios:", error);
    }
}

function initializeMap() {
    map = L.map('map').setView([-4.5, -63], 6);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors © CARTO'
    }).addTo(map);
    geojsonLayer = L.geoJson(null, { style: styleFeature, onEachFeature }).addTo(map);
}

// Funções do Mapa (styleFeature, onEachFeature, getColor, updateLegend)
function styleFeature(feature) {
    const config = metricsConfig[currentMapMetric];
    const value = feature.properties[config.property];
    return {
        fillColor: getColor(value, config.grades, config.colors),
        weight: 1.5, opacity: 1, color: 'white', fillOpacity: 0.8
    };
}
function onEachFeature(feature, layer) {
    const props = feature.properties;
    if (props && props.nome) {
        let popupContent = `<b>${props.nome}</b><br>`;
        for(const metric in metricsConfig){
            const config = metricsConfig[metric];
            const value = props[config.property];
            popupContent += `<b>${config.label}:</b> ${config.format(value)}<br>`;
        }
        layer.bindPopup(popupContent);
    }
    layer.on({
        mouseover: e => e.target.setStyle({ weight: 3, color: '#333' }),
        mouseout: e => geojsonLayer.resetStyle(e.target),
        click: e => map.fitBounds(e.target.getBounds())
    });
}
function getColor(d, grades, colors) {
    if (d === null || d === undefined) return '#cccccc';
    for (let i = grades.length - 1; i >= 0; i--) {
        if (d > grades[i]) return colors[i + 1];
    }
    return colors[0];
}
function updateLegend() {
    if (legendControl) map.removeControl(legendControl);
    const config = metricsConfig[currentMapMetric];
    legendControl = L.control({ position: 'bottomright' });
    legendControl.onAdd = function() {
        const div = L.DomUtil.create('div', 'info legend');
        const grades = config.grades;
        const colors = config.colors;
        div.innerHTML += `<h4>${config.legendTitle}</h4>`;
        for (let i = 0; i < colors.length; i++) {
            const from = grades[i - 1];
            const to = grades[i];
            let label = i === 0 ? `< ${to}` : (i === colors.length - 1 ? `> ${from}` : `${from} &ndash; ${to}`);
            if(grades[i] === undefined && i > 0) label = `> ${from}`;
            div.innerHTML += `<i style="background:${colors[i]}"></i> ${label}<br>`;
        }
        return div;
    };
    legendControl.addTo(map);
}
async function updateRankingCard() {
    const config = metricsConfig[currentMapMetric];
    document.getElementById('ranking-title').textContent = `Destaques por ${config.label}`;
    const rankingContent = document.getElementById('ranking-content');
    rankingContent.innerHTML = '<p style="text-align:center;">Carregando...</p>';
    try {
        const response = await fetch(`/api/ranking/${currentMapMetric}?age_group=${currentAgeGroup}`);
        const data = await response.json();
        const formatValue = config.format;
        const bestLabel = (currentMapMetric === 'vulnerabilidade') ? 'Menores Índices' : 'Maiores Índices';
        const worstLabel = (currentMapMetric === 'vulnerabilidade') ? 'Maiores Índices' : 'Menores Índices';
        let topHtml = `<ul class="ranking-list top"><h4>${bestLabel}</h4>`;
        data.top_5.forEach(item => {
            topHtml += `<li><span class="municipio-name">${item.municipio.charAt(0).toUpperCase() + item.municipio.slice(1).toLowerCase()}</span><span class="municipio-value">${formatValue(item.value)}</span></li>`;
        });
        topHtml += '</ul>';
        let bottomHtml = `<ul class="ranking-list bottom"><h4>${worstLabel}</h4>`;
        data.bottom_5.forEach(item => {
            bottomHtml += `<li><span class="municipio-name">${item.municipio.charAt(0).toUpperCase() + item.municipio.slice(1).toLowerCase()}</span><span class="municipio-value">${formatValue(item.value)}</span></li>`;
        });
        bottomHtml += '</ul>';
        rankingContent.innerHTML = topHtml + bottomHtml;
    } catch (error) {
        console.error("Falha ao carregar ranking:", error);
        rankingContent.innerHTML = '<p style="text-align:center; color: red;">Não foi possível carregar os dados.</p>';
    }
}
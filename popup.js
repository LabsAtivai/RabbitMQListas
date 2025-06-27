let format = 'excel'; // Definição de formato padrão

document.getElementById('extract').addEventListener('click', () => {
  document.getElementById('status').innerText = 'Iniciando extração...';
  document.getElementById('progress-bar').style.width = '0%';
  document.getElementById('format-buttons').style.display = 'none'; // Ocultar opções de formato durante a extração

  // Enviar mensagem para o `background.js` iniciar a extração
  chrome.runtime.sendMessage({ action: 'startExtraction', format }, (response) => {
    document.getElementById('status').innerText = response.status;
  });
});

document.getElementById('csv').addEventListener('click', () => {
  format = 'csv';
  updateFormatButtons();
});

document.getElementById('excel').addEventListener('click', () => {
  format = 'excel';
  updateFormatButtons();
});

// Função para atualizar visualmente os botões de formato
function updateFormatButtons() {
  document.getElementById('csv').style.backgroundColor = format === 'csv' ? '#ff8c33' : '#ff6600';
  document.getElementById('excel').style.backgroundColor = format === 'excel' ? '#ff8c33' : '#ff6600';
}

// Atualizar a barra de progresso com base na porcentagem fornecida
function updateProgressBar(progress) {
  document.getElementById('progress-bar').style.width = `${progress}%`;
}

// Ouvir atualizações do background.js
chrome.runtime.onMessage.addListener((message) => {
  if (message.action === 'updateProgress') {
    updateProgressBar(message.progress);
    if (message.progress === 100) {
      // Extração concluída - Mostrar botões de formato
      document.getElementById('status').innerText = 'Extração completa!';
      document.getElementById('format-buttons').style.display = 'flex'; // Mostrar botões após extração
    }
  }
});

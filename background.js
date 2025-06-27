chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'startExtraction') {
    startExtraction(request.format)
      .then(() => sendResponse({ status: 'Extração concluída!' }))
      .catch((error) => sendResponse({ status: `Erro na extração: ${error.message}` }));
    return true; // Indicar que a resposta será enviada de forma assíncrona
  }
});

async function startExtraction(format) {
  const token = await getAuthToken();
  if (!token) return;

  const headers = new Headers();
  headers.append("Authorization", `Bearer ${token}`);
  
  const labelResponse = await fetch("https://www.googleapis.com/gmail/v1/users/me/labels", { headers });
  const labels = await labelResponse.json();

  if (!labels.labels) {
    console.error("Nenhuma caixa de correio encontrada ou erro na resposta da API", labels);
    return;
  }

  let emailData = {};
  let totalEmails = 0;
  let emailsProcessed = 0;

  async function getMessagesWithPagination(labelId, labelName, pageToken = null) {
    try {
      let url = `https://www.googleapis.com/gmail/v1/users/me/messages?labelIds=${labelId}&maxResults=100`;
      if (pageToken) url += `&pageToken=${pageToken}`;

      const response = await fetch(url, { headers });
      const messages = await response.json();
      
      if (!messages.messages) return;
      
      totalEmails += messages.messages.length;

      await Promise.all(
        messages.messages.map(async (message) => {
          const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${message.id}?format=full`, { headers });
          const msg = await msgRes.json();

          const headers = msg.payload.headers;
          if (!headers) return;

          let snippet = msg.snippet || 'Sem conteúdo de texto disponível';

          const emailDetails = {
            De: headers.find((header) => header.name === "From")?.value || 'Desconhecido',
            Para: headers.find((header) => header.name === "To")?.value || 'Desconhecido',
            Assunto: headers.find((header) => header.name === "Subject")?.value || 'Sem Assunto',
            "Primeiras Frases": snippet,
          };

          if (!emailData[labelName]) {
            emailData[labelName] = [];
          }
          emailData[labelName].push(emailDetails);

          emailsProcessed++;

          const progress = Math.round((emailsProcessed / totalEmails) * 100);
          chrome.runtime.sendMessage({ action: 'updateProgress', progress });
        })
      );

      if (messages.nextPageToken) {
        await getMessagesWithPagination(labelId, labelName, messages.nextPageToken);
      }
    } catch (error) {
      console.error(`Erro ao processar mensagens para a label ${labelName}`, error);
    }
  }

  await Promise.all(
    labels.labels.map(async (label) => {
      await getMessagesWithPagination(label.id, label.name);
    })
  );

  console.warn("Extração Completa", emailData);
  chrome.runtime.sendMessage({ action: 'updateProgress', progress: 100 });
}

async function getAuthToken() {
  return new Promise((resolve) => {
    chrome.identity.getAuthToken({ interactive: true }, (token) => {
      if (chrome.runtime.lastError) {
        console.error("Erro ao obter token de autenticação:", chrome.runtime.lastError);
        resolve(null);
      } else {
        resolve(token);
      }
    });
  });
}

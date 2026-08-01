(() => {
  document.querySelectorAll("form").forEach((form) => {
    form.addEventListener("submit", () => {
      const button = form.querySelector("button[type=submit]");
      if (!button) return;
      button.disabled = true;
      button.dataset.label = button.textContent;
      button.textContent = "Проверяем…";
    });
  });
})();

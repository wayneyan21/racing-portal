async function loadHorses() {
  const res = await fetch('/flask/api/horses');
  const data = await res.json();
  console.log(data);
}
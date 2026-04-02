import { useEffect, useRef } from 'react'
import mermaid from 'mermaid'

interface MermaidDiagramProps {
  schema: string
}

function MermaidDiagram({ schema }: MermaidDiagramProps) {
  const mermaidRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    mermaid.initialize({
      startOnLoad: false,
      theme: 'default',
      securityLevel: 'loose',
    })
  }, [])

  useEffect(() => {
    const renderDiagram = async () => {
      if (mermaidRef.current) {
        try {
          mermaidRef.current.innerHTML = ''
          const id = `mermaid-${Date.now()}`
          const { svg } = await mermaid.render(id, schema)
          if (mermaidRef.current) {
            mermaidRef.current.innerHTML = svg
          }
        } catch (error) {
          console.error('Erreur lors du rendu Mermaid:', error)
          if (mermaidRef.current) {
            mermaidRef.current.innerHTML = '<div class="text-red-500">Erreur lors du chargement du diagramme</div>'
          }
        }
      }
    }
    renderDiagram()
  }, [schema])

  return (
    <div className="bg-gray-50 rounded-lg p-6 border border-gray-200 overflow-auto w-full">
      <div ref={mermaidRef} className="mermaid-container w-full flex justify-center" style={{ minHeight: '600px' }}></div>
    </div>
  )
}

export default MermaidDiagram
